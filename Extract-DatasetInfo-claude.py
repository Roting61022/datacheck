# -*- coding: utf-8 -*-
"""
Extract-DatasetInfo-Final-Fast.py
- XML 优先，跳过 <sysout>，仅输出含 <datasetName> 的 assign
- 合并 JCL(CDATA) 信息补全 LEN/FORMAT/RETPD/TAPE
- 结束后做 SHR 长度回填（同名 DSN 存在 NEW 且有 LEN）
- 从 PGM 源码（固定路径）解析：
    * PGM_Len: 匹配 ddName 的 @CBLFile(... recLen=NNN)
    * COPY句 : 先数字包含匹配，再按约定 <前两字母>+'R'+<数字前5位> 命中（例：FA781039→FAR78103）
    * copy_Len: 从 COPY 源码计算总字节长度（基于 @Hensu 的 pic/usage）
- 支持 <options> 节点的 PGM/COPYCLASS 提取（如 JXZOROG）
- 自然排序：按 JOB名 -> STEP -> DD名
- 并行处理（多进程）

输出列顺序：
  JOB名, STEP, プログラム, DD名, PGM_Len, COPY句, copy_Len, ファイル／DB名, DISP1, NORMAL, ABNORMAL, LEN, FORMAT, RETPD, TAPE
"""

import os
import re
import csv
import gzip
import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from xml.etree import ElementTree as ET

# ================= 固定路径（可按需改/注释） =================
FIXED_INPUT    = r"C:\Users\yuanzhe.feng\Documents\0件\#87510対応_パラメータファイル修正\hitachihost_job_r4.3\jobs\jobs_xml"
FIXED_OUTPUT   = r"C:\Users\yuanzhe.feng\Documents\0件\GIT\test_result"
FIXED_PGM_SRC  = r"C:\Users\yuanzhe.feng\Documents\0件\GIT\PGM"   # PGM 源码根目录（如不需要，置空 "" 或前面加 # 注释）
FIXED_COPY_SRC = r"C:\Users\yuanzhe.feng\Documents\0件\GIT\copy"  # COPY 句源码根目录（用于计算长度）
WORKERS = os.cpu_count() or 4
VERBOSE = True
USE_GZIP = True  # 是否使用 GZIP 压缩输出（可减小文件大小 70-90%）

# ================= COPY 长度计算相关常量 =================
NATIONAL_CHAR_BYTES = 2  # PIC N(n)
DBCS_CHAR_BYTES = 2      # PIC G(n)

# ================= 正则 =================
RE_EXEC        = re.compile(r'^\s*//\s*(?P<step>[A-Z0-9$#@]+)\s+EXEC\b.*?\bPGM\s*=\s*(?P<pgm>[A-Z0-9_]+)', re.I)
RE_DD_HEAD     = re.compile(r'^\s*//\s*(?P<dd>[A-Z0-9$#@]+)\s+DD\b', re.I)
RE_LRECL       = re.compile(r'\bLRECL\s*=\s*(\d+)\b', re.I)
RE_RECFM       = re.compile(r'\bRECFM\s*=\s*([A-Z]+)\b', re.I)
RE_DSN         = re.compile(r"\bDSN\s*=\s*'?([A-Za-z0-9.&\-\$@#\?_()]+)'?", re.I)
RE_DISP_TUP    = re.compile(r'\bDISP\s*=\s*\(([^)]*)\)', re.I)    # DISP=(NEW,CATLG,DELETE)
RE_DISP_1      = re.compile(r'\bDISP\s*=\s*([A-Z]+)\b', re.I)     # DISP=SHR
RE_LABEL_RETPD = re.compile(r'\bRETPD\s*=\s*(\d+)\b', re.I)
RE_UNIT        = re.compile(r'\bUNIT\s*=\s*([A-Z0-9]+)\b', re.I)
RE_CDATA_ALL   = re.compile(r'<!\[CDATA\[(.*?)\]\]>', re.S | re.I)

RE_XMLNS = 'http://www.majalis.amo.accenture.com/schema/jobsettings'

# Java 解析
def RE_CBLFILE_FOR_DD(dd):
    return re.compile(
        r'@CBLFile\s*\([^)]*ddName\s*=\s*"' + re.escape(dd) + r'"\s*[^)]*recLen\s*=\s*(\d+)',
        re.I | re.S
    )
RE_HENSU_COPYNAME = re.compile(r'@Hensu\s*\([^)]*isCopy\s*=\s*true[^)]*name\s*=\s*"([A-Za-z0-9_]+)"', re.I | re.S)

PGM_EXCLUDES = {"SORT", "COPY", "GREEN", "SORTIN", "IEBGENER", "IDCAMS", "ICETOOL", "IEFBR14"}

# ================= COPY 长度计算正则 =================
# 从 @Hensu 注解中提取 pic 和 usage
RE_HENSU_PIC = re.compile(
    r'@Hensu\s*\([^)]*\bpic\s*=\s*"(?P<pic>[^"]+)"(?:[^)]*\busage\s*=\s*"(?P<usage>[^"]+)")?',
    re.I | re.S
)
# PIC 通用格式：X/9/N/G(n)
RE_PIC_GENERAL = re.compile(r'^\s*(S)?\s*([Xx9NnGg])\s*\(\s*(\d+)\s*\)\s*$')
# PIC 9(n)V9(m) 格式
RE_PIC_9V = re.compile(r'^\s*(S)?\s*9\s*\(\s*(\d+)\s*\)\s*(?:V\s*9\s*\(\s*(\d+)\s*\))?\s*$', re.I)

# ================= 工具函数 =================
def natural_sort_key(text: str):
    """
    自然排序键函数：将字符串分割为文本和数字部分
    例如：'AFAJ113X' -> ['AFAJ', 113, 'X']
    这样可以正确排序：AFAJ112X < AFAJ113X < AFAJ114X
    """
    def convert(part):
        return int(part) if part.isdigit() else part.lower()
    return [convert(c) for c in re.split(r'(\d+)', text)]

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def iter_target_xml_files(root_dir: str):
    for dirpath, _, filenames in os.walk(root_dir):
        for fn in filenames:
            if fn.lower().endswith('jobsettingparams.xml'):
                yield os.path.join(dirpath, fn)

def read_text(path):
    try:
        with open(path, 'r', encoding='utf-8-sig', errors='replace') as f:
            return f.read()
    except Exception:
        with open(path, 'r', encoding=None, errors='replace') as f:
            return f.read()

def get_jobname_from_root_attr(root, default_name):
    if root is not None:
        jn = root.attrib.get('jobName')
        if jn:
            return jn[:8]
    return default_name[:8]

def parse_jcl_struct(cdata_text: str):
    step_map = {}
    current_step = None
    current_pgm = None
    lines = cdata_text.splitlines()

    for idx, ln in enumerate(lines):
        m_exec = RE_EXEC.match(ln)
        if m_exec:
            current_step = m_exec.group('step')
            current_pgm  = m_exec.group('pgm')
            step_map.setdefault(current_step, {"pgm": current_pgm, "dds": {}})
            continue

        m_dd = RE_DD_HEAD.match(ln)
        if m_dd and current_step:
            dd = m_dd.group('dd')
            # 读取当前行及所有后续延续行
            window = ln
            j = idx + 1
            while j < len(lines):
                next_line = lines[j]
                stripped = next_line.strip()

                # 跳过空行
                if not stripped:
                    j += 1
                    continue

                # 检查是否是 JCL 行
                if '//' in next_line:
                    # 找到 // 的位置
                    dd_pos = next_line.find('//')
                    after_slashes = next_line[dd_pos+2:] if dd_pos + 2 < len(next_line) else ""

                    # 延续行：// 后面直接是空格（不是新的 DD/EXEC 名称）
                    if after_slashes and after_slashes[0] == ' ':
                        window += "\n" + next_line
                        j += 1
                    else:
                        # 新的 DD/EXEC 语句
                        break
                else:
                    # 非 JCL 行
                    break

            dsn = ""
            m_dsn = RE_DSN.search(window)
            if m_dsn:
                dsn = m_dsn.group(1).strip().lstrip("&").strip("'")

            disp1 = normal = abnormal = ""
            m_disp_t = RE_DISP_TUP.search(window)
            if m_disp_t:
                parts = [p.strip().upper() for p in m_disp_t.group(1).split(',')]
                if len(parts) > 0: disp1 = parts[0]
                if len(parts) > 1: normal = parts[1]
                if len(parts) > 2: abnormal = parts[2]
            else:
                m_disp1 = RE_DISP_1.search(window)
                if m_disp1:
                    disp1 = m_disp1.group(1).upper()

            length = ""
            fmt = ""
            m_len = RE_LRECL.search(window)
            if m_len:
                length = m_len.group(1)
            m_fmt = RE_RECFM.search(window)
            if m_fmt:
                fmt = m_fmt.group(1).upper()

            retpd = ""
            m_ret = RE_LABEL_RETPD.search(window)
            if m_ret: retpd = m_ret.group(1)

            tape = ""
            m_unit = RE_UNIT.search(window)
            if m_unit:
                unit = m_unit.group(1).upper()
                if unit in ("MTL", "TAPE"):
                    tape = "true"
                elif unit in ("DISK",):
                    tape = "NO"

            step_map.setdefault(current_step, {"pgm": current_pgm, "dds": {}})
            step_map[current_step]["dds"][dd] = {
                "dsn": dsn or "",
                "disp1": disp1,
                "normal": normal,
                "abnormal": abnormal,
                "len": length,
                "fmt": fmt,
                "retpd": retpd,
                "tape": tape
            }
    return step_map

def merge_dict_of_steps(total_map, part_map):
    for stp, info in part_map.items():
        if stp not in total_map:
            total_map[stp] = {"pgm": info.get("pgm"), "dds": {}}
        if not total_map[stp].get("pgm"):
            total_map[stp]["pgm"] = info.get("pgm")
        total_map[stp]["dds"].update(info.get("dds", {}))

def choose_xml_pgm(step_elem):
    # 优先级：execKahawaPgm -> execUtility -> execPgm
    for attr in ("execKahawaPgm", "execUtility", "execPgm"):
        v = step_elem.attrib.get(attr, "")
        if v:
            return v.strip()
    return ""

# ===== COPY 长度计算函数 =====
def split_pic_and_usage(pic_token: str) -> tuple:
    """分离 PIC 和内联的 USAGE（如 'S9(5) COMP-3'）"""
    t = pic_token.strip()
    if RE_PIC_GENERAL.match(t) or RE_PIC_9V.match(t):
        return t, ""
    parts = t.split()
    if len(parts) >= 2:
        for k in range(len(parts), 0, -1):
            pic_try = " ".join(parts[:k]).strip()
            rest = " ".join(parts[k:]).strip()
            if RE_PIC_GENERAL.match(pic_try) or RE_PIC_9V.match(pic_try):
                return pic_try, rest.upper()
    return t, ""

def parse_pic_core(pic_core: str):
    """解析 PIC，返回 (signed, kind, total_digits, dec_digits)"""
    t = pic_core.strip()
    m = RE_PIC_GENERAL.match(t)
    if m:
        signed = bool(m.group(1))
        kind = m.group(2).upper()
        n = int(m.group(3))
        return signed, kind, n, 0
    m2 = RE_PIC_9V.match(t)
    if m2:
        signed = bool(m2.group(1))
        kind = '9'
        int_digits = int(m2.group(2))
        dec_digits = int(m2.group(3)) if m2.group(3) else 0
        return signed, kind, int_digits + dec_digits, dec_digits
    raise ValueError(f"Unsupported PIC: {pic_core}")

def bytes_binary(d: int) -> int:
    """二进制（COMP/COMP-4/COMP-5）字节数"""
    if d <= 4: return 2
    if d <= 9: return 4
    if d <= 18: return 8
    raise ValueError(f"Binary COMP digits too large: {d}")

def bytes_packed(d: int) -> int:
    """COMP-3（Packed Decimal）字节数"""
    return (d + 2) // 2

def size_for_pic(pic_token: str, usage_token: str = "") -> int:
    """计算 PIC 字段的字节长度"""
    pic_core, inline_usage = split_pic_and_usage(pic_token)
    signed, kind, n_total, dec = parse_pic_core(pic_core)
    u = (usage_token or "").strip().upper()
    if not u:
        u = inline_usage

    if kind == 'X':
        return n_total
    if kind == 'N':
        return n_total * NATIONAL_CHAR_BYTES
    if kind == 'G':
        return n_total * DBCS_CHAR_BYTES
    if kind == '9':
        if u in ('', 'DISPLAY'):
            return n_total
        if u in ('COMP', 'BINARY', 'COMP-5', 'COMP-4'):
            return bytes_binary(n_total)
        if u in ('COMP-3', 'PACK', 'PACKED-DECIMAL', 'PACKED'):
            return bytes_packed(n_total)
        return n_total
    raise ValueError(f"Unsupported kind: {kind}")

def calculate_copy_length(java_text: str) -> int:
    """从 COPY 的 Java 文件中计算总长度"""
    if not java_text:
        return 0
    total = 0
    for m in RE_HENSU_PIC.finditer(java_text):
        pic = m.group('pic').strip()
        usage = (m.group('usage') or '').strip()
        try:
            sz = size_for_pic(pic, usage)
            total += sz
        except Exception:
            # 忽略无法解析的字段
            pass
    return total

def build_copy_index(src_root: str):
    """构建 COPY 句索引：{copy名.lower(): 文件路径}"""
    index = {}
    if not src_root or not os.path.isdir(src_root):
        return index
    for dirpath, _, filenames in os.walk(src_root):
        for fn in filenames:
            if fn.lower().endswith(".java"):
                copy_name = fn[:-5].lower()  # 去掉 .java
                index[copy_name] = os.path.join(dirpath, fn)
    return index

def get_copy_length(copy_index: dict, copy_name: str) -> str:
    """获取 COPY 句的长度"""
    if not copy_name or not copy_index:
        return ""
    path = copy_index.get(copy_name.lower())
    if not path or not os.path.isfile(path):
        return ""
    try:
        java_text = read_text(path)
        length = calculate_copy_length(java_text)
        return str(length) if length > 0 else ""
    except Exception:
        return ""

# ===== Java 索引/解析 =====
def build_java_index(src_root: str):
    index = {}
    if not src_root or not os.path.isdir(src_root):
        return index
    for dirpath, _, filenames in os.walk(src_root):
        for fn in filenames:
            if fn.lower().endswith(".java"):
                index[fn.lower()] = os.path.join(dirpath, fn)
    return index

def load_java_file(java_index: dict, pgm: str):
    if not pgm: return ""
    path = java_index.get(f"{pgm.lower()}.java")
    if not path or not os.path.isfile(path):
        return ""
    return read_text(path)

def extract_pgm_len_from_java(java_text: str, dd_name: str):
    if not java_text or not dd_name:
        return ""
    m = RE_CBLFILE_FOR_DD(dd_name).search(java_text)
    if m:
        return m.group(1)
    return ""

def is_dd_defined_in_cblfile(java_text: str, dd_name: str) -> bool:
    """检查 DD 名称是否在 @CBLFile 中定义"""
    if not java_text or not dd_name:
        return False
    pattern = re.compile(
        r'@CBLFile\s*\([^)]*ddName\s*=\s*"' + re.escape(dd_name) + r'"',
        re.I | re.S
    )
    return pattern.search(java_text) is not None

def derive_expected_copy_from_dd(dd_name: str) -> str:
    """
    依据命名约定：第二个字母后加 'R'，取后续数字的前5位。
    例如：FA781039 -> FAR78103,  FE83001I -> FER83001
    """
    if not dd_name:
        return ""
    m = re.match(r'^([A-Z]{2})(\d+)([A-Z0-9]?)$', dd_name.upper())
    if not m:
        return ""
    letters, digits, _tail = m.groups()
    if len(digits) < 5:
        return ""
    return f"{letters}R{digits[:5]}"

def best_copy_name(java_text: str, dd_name: str) -> str:
    """
    COPY句 解析优先级：
    1) 提取 dd 的数字串，派生候选（完整、前5位、去掉最后1位），用包含匹配命中 @Hensu(name=...)
    2) 使用命名规律 <前两字母>+'R'+<数字前5位> 命中；若存在前后缀，允许作为子串命中
    """
    if not java_text or not dd_name:
        return ""
    copies = RE_HENSU_COPYNAME.findall(java_text)
    if not copies:
        return ""

    dd_u = dd_name.upper()
    mnum = re.search(r'(\d+)', dd_u)
    candidates = []
    if mnum:
        d = mnum.group(1)
        candidates.append(d)           # 全数字
        if len(d) >= 5:
            candidates.append(d[:5])   # 前5位
        if len(d) >= 2:
            candidates.append(d[:-1])  # 去掉尾1位

    # 去重与排序（长的优先）
    seen = set()
    cand_nums = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            cand_nums.append(c)
    cand_nums.sort(key=lambda x: (-len(x), x))

    # 先用数字包含匹配
    for n in cand_nums:
        for name in copies:
            if n in name:
                return name

    # 再按命名规律
    expected = derive_expected_copy_from_dd(dd_u)
    if expected:
        for name in copies:
            if name.upper() == expected:
                return name
        for name in copies:
            if expected in name.upper():
                return name

    return ""

# ===== 单文件处理 =====
def process_one_xml(file_path: str, java_index=None):
    rows = []

    content = read_text(file_path)
    if not content.strip():
        return rows, {"FilePath": file_path, "JobName": "", "Rows": 0}

    job_name = os.path.splitext(os.path.basename(file_path))[0]
    try:
        start_root = ET.fromstring(content[content.find('<'):])
        jn = start_root.attrib.get('jobName')
        if jn:
            job_name = jn[:8]
    except Exception:
        pass

    # JCL 聚合
    jcl_struct = {}
    for cdm in RE_CDATA_ALL.finditer(content):
        part = parse_jcl_struct(cdm.group(1))
        merge_dict_of_steps(jcl_struct, part)

    # XML
    ns = {'m': RE_XMLNS}
    try:
        root = ET.fromstring(content[content.find('<'):])
    except Exception:
        root = None

    if root is not None:
        for step in root.findall('.//m:step', ns):
            step_name = step.attrib.get('stepName', '').strip() or ""
            # PGM：XML 优先，否则回退 JCL
            pgm_xml = choose_xml_pgm(step)
            pgm_jcl = (jcl_struct.get(step_name, {}).get("pgm") if step_name in jcl_struct else "") or ""
            pgm = pgm_xml or pgm_jcl

            # 满足 8 位英数且不在工具排除名单才查 Java
            can_java = bool(re.fullmatch(r'[A-Z0-9]{8}', pgm or "")) and (pgm not in PGM_EXCLUDES)
            java_text = load_java_file(java_index, pgm) if (can_java and java_index) else ""

            # 处理 <options> 节点（如 JXZOROG 调用 PGM/COPYCLASS）
            options_node = step.find('m:options', ns)
            option_pgm = ""
            option_copyclass = ""
            if options_node is not None:
                for opt in options_node.findall('m:option', ns):
                    opt_name = (opt.get('name') or "").strip().upper()
                    opt_value = (opt.text or "").strip()
                    if opt_name == "PGM":
                        option_pgm = opt_value
                    elif opt_name == "COPYCLASS":
                        # COPYCLASS 可能是 "GKRZK023.GKRZK023"，取最后一段
                        if '.' in opt_value:
                            option_copyclass = opt_value.split('.')[-1]
                        else:
                            option_copyclass = opt_value

            # 如果 options 中有 PGM，添加一个额外的行
            if option_pgm:
                rows.append({
                    "JOB名": job_name,
                    "STEP": step_name,
                    "プログラム": option_pgm,
                    "DD名": "",
                    "PGM_Len": "",
                    "COPY句": option_copyclass,
                    "copy_Len": "",  # 稍后统一填充
                    "ファイル／DB名": "",
                    "DISP1": "",
                    "NORMAL": "",
                    "ABNORMAL": "",
                    "LEN": "",
                    "FORMAT": "",
                    "RETPD": "",
                    "TAPE": ""
                })

            for assign in step.findall('m:assign', ns):
                # 跳过 sysout
                if assign.find('m:sysout', ns) is not None:
                    continue

                dd_name   = (assign.get('ddName') or "").strip()
                dsn_node  = assign.find('m:datasetName', ns)
                if dsn_node is None or not (dsn_node.text or "").strip():
                    continue  # 只有 datasetName 的才出力

                disp_node = assign.find('m:disp', ns)
                norm_node = assign.find('m:normal', ns)
                abn_node  = assign.find('m:abnormal', ns)
                len_node  = assign.find('m:len', ns)
                ret_node  = assign.find('m:retpd', ns)
                tape_node = assign.find('m:tape', ns)

                dsn  = (dsn_node.text or "").strip()
                disp = ((disp_node.text or "").strip().upper()) if disp_node is not None else ""
                normal   = ((norm_node.text or "").strip().upper()) if norm_node is not None else ""
                abnormal = ((abn_node.text or "").strip().upper()) if abn_node is not None else ""
                x_len = (len_node.text or "").strip() if len_node is not None else ""
                retpd = (ret_node.text or "").strip() if ret_node is not None else ""
                tape  = (tape_node.text or "").strip().lower() if tape_node is not None else ""

                # 合并 JCL（同 step：先 dd 再 dsn）
                j_dds = jcl_struct.get(step_name, {}).get("dds", {}) if step_name else {}
                j_hit = None
                if dd_name and dd_name in j_dds:
                    j_hit = j_dds[dd_name]
                elif dsn:
                    for _dd, info in j_dds.items():
                        if info.get("dsn", "") == dsn:
                            j_hit = info
                            break

                disp1   = (disp or (j_hit.get("disp1") if j_hit else "") or "").upper()
                normal  = (normal or (j_hit.get("normal") if j_hit else "") or "").upper()
                abnormal= (abnormal or (j_hit.get("abnormal") if j_hit else "") or "").upper()
                length  = (j_hit.get("len") if j_hit else "") or x_len or ""
                fmt     = ((j_hit.get("fmt") if j_hit else "") or "").upper()
                retpd_v = retpd or (j_hit.get("retpd") if j_hit else "") or ""
                tape_v  = ""
                if tape:
                    tape_v = "true" if tape.lower() == "true" else ("NO" if tape.lower() == "no" else tape)
                elif j_hit:
                    tape_v = j_hit.get("tape") or ""

                # PGM_Len / COPY句
                pgm_len = ""
                copy_name = ""
                if java_text:
                    pgm_len = extract_pgm_len_from_java(java_text, dd_name) or ""
                    # 只有在 @CBLFile 中定义的 DD 才提取 COPY 句
                    if is_dd_defined_in_cblfile(java_text, dd_name):
                        copy_name = best_copy_name(java_text, dd_name) or ""

                rows.append({
                    "JOB名": job_name,
                    "STEP": step_name,
                    "プログラム": pgm,
                    "DD名": dd_name,
                    "PGM_Len": pgm_len,
                    "COPY句": copy_name,
                    "copy_Len": "",  # 稍后统一填充
                    "ファイル／DB名": dsn,
                    "DISP1": disp1,
                    "NORMAL": normal,
                    "ABNORMAL": abnormal,
                    "LEN": length,
                    "FORMAT": fmt,
                    "RETPD": retpd_v,
                    "TAPE": tape_v
                })

    return rows, {"FilePath": file_path, "JobName": job_name, "Rows": len(rows)}

def backfill_shr_lengths(all_rows):
    new_len_map = {}
    for r in all_rows:
        if r.get("DISP1") == "NEW":
            dsn = r.get("ファイル／DB名") or ""
            ln  = r.get("LEN") or ""
            if dsn and ln and ln.isdigit():
                ln_i = int(ln)
                if dsn not in new_len_map or ln_i > new_len_map[dsn]:
                    new_len_map[dsn] = ln_i
    for r in all_rows:
        if r.get("DISP1") == "SHR" and not (r.get("LEN") or "").strip():
            dsn = r.get("ファイル／DB名") or ""
            if dsn in new_len_map:
                r["LEN"] = str(new_len_map[dsn])

def fill_copy_lengths(all_rows, copy_index):
    """填充所有行的 copy_Len"""
    for r in all_rows:
        copy_name = r.get("COPY句") or ""
        if copy_name:
            r["copy_Len"] = get_copy_length(copy_index, copy_name)

def main():
    input_path  = FIXED_INPUT
    output_path = FIXED_OUTPUT
    pgm_src_root= FIXED_PGM_SRC
    workers     = WORKERS
    global VERBOSE
    VERBOSE = True

    ensure_dir(output_path)
    files = list(iter_target_xml_files(input_path))
    if not files:
        print(f"❌ No *jobsettingParams.xml under: {input_path}")
        return
    if VERBOSE:
        print(f"✅ Target XML files: {len(files)}")

    # 主进程构建一次 Java 索引；传给子进程
    java_index = build_java_index(pgm_src_root) if pgm_src_root else {}
    if VERBOSE and pgm_src_root:
        print(f"✅ Java sources indexed: {len(java_index)} files under: {pgm_src_root}")

    # 构建 COPY 索引（用于计算长度）
    copy_src_root = FIXED_COPY_SRC
    copy_index = build_copy_index(copy_src_root) if copy_src_root else {}
    if VERBOSE and copy_src_root:
        print(f"✅ COPY sources indexed: {len(copy_index)} files under: {copy_src_root}")

    all_rows = []
    all_summary = []

    processed = 0
    with ProcessPoolExecutor(max_workers=max(1, workers)) as exe:
        futures = {exe.submit(process_one_xml, fp, java_index): fp for fp in files}
        for fut in as_completed(futures):
            fp = futures[fut]
            try:
                rows, summary = fut.result()
                all_rows.extend(rows)
                all_summary.append(summary)
                processed += 1
                if VERBOSE and processed % 100 == 0:
                    print(f"Progress: {processed}/{len(files)} files processed ({processed*100//len(files)}%)")
            except Exception as e:
                if VERBOSE:
                    print(f"[WARN] {fp} -> {e}")
                processed += 1

    # SHR 回填
    backfill_shr_lengths(all_rows)

    # ===== 自然排序：按 JOB名 -> STEP -> DD名 =====
    all_rows.sort(key=lambda r: (
        natural_sort_key(r.get("JOB名") or ""),
        natural_sort_key(r.get("STEP") or ""),
        natural_sort_key(r.get("DD名") or "")
    ))

    # 填充 COPY 长度
    if copy_index:
        fill_copy_lengths(all_rows, copy_index)
        if VERBOSE:
            print(f"✅ COPY lengths calculated")

    # ===== 输出 =====
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("\n========================================")
    print(f"✅ Complete. Files: {len(files)} | Rows: {len(all_rows)}")
    print("========================================")

    # 输出详细 CSV（支持 GZIP 压缩）
    base_csv = 'dataset_detail.csv'
    out_csv = os.path.join(output_path, base_csv + ('.gz' if USE_GZIP else ''))
    headers = ["JOB名","STEP","プログラム","DD名","PGM_Len","COPY句","copy_Len","ファイル／DB名","DISP1","NORMAL","ABNORMAL","LEN","FORMAT","RETPD","TAPE"]

    if USE_GZIP:
        with gzip.open(out_csv, 'wt', newline='', encoding='utf-8') as fw:
            w = csv.DictWriter(fw, fieldnames=headers, lineterminator='\n')
            w.writeheader()
            w.writerows(all_rows)
    else:
        with open(out_csv, 'w', newline='', encoding='utf-8') as fw:
            w = csv.DictWriter(fw, fieldnames=headers, lineterminator='\n')
            w.writeheader()
            w.writerows(all_rows)

    if VERBOSE:
        file_size = os.path.getsize(out_csv) / (1024 * 1024)  # MB
        print(f"Detail CSV  : {out_csv} ({file_size:.2f} MB)")

    stats_csv = os.path.join(output_path, 'dataset_statistics.csv')
    with open(stats_csv, 'w', newline='', encoding='utf-8') as fw:
        w = csv.DictWriter(fw, fieldnames=["FilePath","JobName","Rows","GeneratedAt"])
        for row in all_summary:
            r = dict(row); r["GeneratedAt"] = now
            w.writerow(r)
    if VERBOSE:
        print(f"Statistics  : {stats_csv}")

if __name__ == '__main__':
    main()
