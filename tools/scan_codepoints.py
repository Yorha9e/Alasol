#!/usr/bin/env python3
"""扫描本次改动引入行里的**非预期码点**（乱码/生僻字自查）。

用法：python tools/scan_codepoints.py [--base HEAD]

口径（两条腿）：
  ① **白名单腿**：非 ASCII 字符里，只允许 CJK 常用区（U+4E00–U+9FFF）、CJK 扩展 A
     （U+3400–U+4DBF）、CJK 兼容汉字（U+F900–U+FAFF）、CJK 标点（U+3000–U+303F）、
     全角形式（U+FF00–U+FFEF）、通用标点（U+2000–U+206F），以及本仓常用的少量符号
     （✓ ✗ ⚠ ✅ ①…⑳ ≠ ≈ ≤ ≥ × ° … — 等）。
  ② **新码点腿**（真正抓乱码的那条）：**基线里从没出现过的**字符一律报出来。
     乱码的典型形态是"**长得像汉字的错字**"（例：把"积累"写成"紶積"）——它照样落在
     CJK 常用区，靠 ① 抓不到；但它是**仓库里第一次出现**的码点，靠 ② 一抓一个准。

范围：git diff 的新增行 + 未跟踪文件（.go/.md/.toml/.json/.txt/.yaml/.yml）的全部行。

退出码：有非预期码点 → 1；否则 0。
"""
import subprocess
import sys
import os
import unicodedata

# 本仓常用的符号（圈码、勾叉、比较符等）——它们在基线里大量出现，白名单显式列出。
ALLOWED_SYMBOLS = set(
    "✓✗⚠⚡✅★☆←→↑↓≈≠≡≤≥×÷°…—–「」『』§¶†‡•‰“”‘’"
    "①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱⑲⑳"
    # 本次 diff **刻意**引入的新词用字（人工确认过、不是乱码）：
    #   愈 —— "自愈"（Writer 文件没了就重建；缺口 5 的 F2 口径）
    #   捏 —— "拿捏"（host/config.go 的 Retention 措辞）
    "愈捏"
)

RANGES = [
    (0x4E00, 0x9FFF),   # CJK 统一表意文字
    (0x3400, 0x4DBF),   # CJK 扩展 A
    (0xF900, 0xFAFF),   # CJK 兼容汉字
    (0x3000, 0x303F),   # CJK 标点
    (0xFF00, 0xFFEF),   # 全角形式
    (0x2000, 0x206F),   # 通用标点
]


def in_white_list(ch: str) -> bool:
    if ord(ch) < 0x80:
        return True
    if ch in ALLOWED_SYMBOLS:
        return True
    cp = ord(ch)
    return any(lo <= cp <= hi for lo, hi in RANGES)


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], capture_output=True, text=True, encoding="utf-8", errors="replace"
    ).stdout


def baseline_chars() -> set:
    """基线（HEAD 已提交内容）里出现过的全部非 ASCII 字符。"""
    chars = set()
    files = git("ls-tree", "-r", "--name-only", "HEAD").splitlines()
    for rel in files:
        if not rel.endswith((".go", ".md", ".toml", ".json", ".txt", ".yaml", ".yml")):
            continue
        blob = subprocess.run(
            ["git", "show", f"HEAD:{rel}"], capture_output=True, text=True,
            encoding="utf-8", errors="replace",
        ).stdout
        for ch in blob:
            if ord(ch) >= 0x80:
                chars.add(ch)
    return chars


def scan_text(path: str, text: str, known: set, findings: list) -> None:
    for lineno, line in enumerate(text.splitlines(), 1):
        for ch in line:
            if ord(ch) < 0x80:
                continue
            reasons = []
            if not in_white_list(ch):
                reasons.append("不在白名单")
            if ch not in known and ch not in ALLOWED_SYMBOLS:
                # 白名单里的"刻意引入的新用字"不算问题；其余新码点一律报出来人工判。
                reasons.append("基线里从没出现过（新码点）")
            if not reasons:
                continue
            name = unicodedata.name(ch, "?")
            findings.append((path, lineno, ch, hex(ord(ch)), name, "、".join(reasons), line.strip()[:120]))


def main() -> int:
    base = "HEAD"
    if "--base" in sys.argv:
        base = sys.argv[sys.argv.index("--base") + 1]
    known = baseline_chars()
    findings: list = []

    # ① 已跟踪文件的改动行。
    cur_file = ""
    for line in git("diff", "-U0", base).splitlines():
        if line.startswith("+++ b/"):
            cur_file = line[6:]
        elif line.startswith("+") and not line.startswith("+++"):
            scan_text(cur_file, line[1:], known, findings)

    # ② 未跟踪文件（全行扫）。
    for rel in git("ls-files", "--others", "--exclude-standard").splitlines():
        if not rel.endswith((".go", ".md", ".toml", ".json", ".txt", ".yaml", ".yml")):
            continue
        try:
            with open(rel, encoding="utf-8") as f:
                scan_text(rel, f.read(), known, findings)
        except (OSError, UnicodeDecodeError):
            continue

    if not findings:
        print("codepoint scan: 无非预期码点")
        return 0
    print(f"codepoint scan: {len(findings)} 处非预期码点")
    for path, lineno, ch, cp, name, why, ctx in findings:
        print(f"  {path}:{lineno}: {ch!r} {cp} {name} [{why}] | {ctx}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
