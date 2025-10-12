
import csv
import sys
from pathlib import Path

# defaults:
DEF_IN = Path("0.tsv")
DEF_V = Path("videos.csv")
DEF_R = Path("related.csv")

def strip_outer_quotes(line: str) -> str:
    """Remove exactly one pair of outer quotes if the entire line is quoted."""
    line = line.rstrip("\r\n")
    if len(line) >= 2 and line[0] == '"' and line[-1] == '"':
        return line[1:-1]
    return line

def clean_id(s: str) -> str:
    """For IDs remove BOM and trailing quotes."""
    if s is None:
        return ""
    s = s.strip()
    # Remove UTF-8 BOM 
    if s and s[0] == "\ufeff":
        s = s[1:]
    # Remove all trailing quotes around the ID
    s = s.lstrip('"').rstrip('"')
    return s

def clean_cell(s: str) -> str:
    """For non-ID cells keep real inner quotes unquote one surrounding pair."""
    if s is None:
        return ""
    s = s.strip()
    if s and s[0] == "\ufeff":
        s = s[1:]
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        s = s[1:-1].replace('""', '"')
    return s

def parse_tsv_rows(fin):
    """
    Source TSV has entire lines quoted and tab-separated inside.
    Remove the outer quotes, then split on literal tabs.
    """
    for raw in fin:
        raw = strip_outer_quotes(raw)
        if not raw:
            continue
        yield raw.split("\t")

def tsv_to_neo_csv(tsv_path: Path, videos_out: Path, related_out: Path) -> None:
    with tsv_path.open("r", encoding="utf-8", newline="") as fin, \
         videos_out.open("a", encoding="utf-8", newline="") as fv, \
         related_out.open("a", encoding="utf-8", newline="") as fr:

        vwriter = csv.writer(fv, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
        rwriter = csv.writer(fr, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")

        if (tsv_path == Path("0.tsv")):
            vwriter.writerow(["videoId","uploader","age","category","length","views","rating","ratingCount","commentCount"])
            rwriter.writerow(["srcVideoId","dstVideoId"])

        for row in parse_tsv_rows(fin):
            if not row:
                continue

            # at least 9 columns for the videos row
            if len(row) < 9:
                row = row + [""] * (9 - len(row))

            video_id = clean_id(row[0])

            # uploader, age, category, length, views, rating, ratingCount, commentCount
            data = [clean_cell(c) for c in row[1:9]]

            vwriter.writerow([video_id] + data)

            # Related IDs columns 9+ (dst video IDs)
            for dst_raw in row[9:]:
                dst = clean_id(dst_raw)
                if dst:  
                    rwriter.writerow([video_id, dst])

def main():

    #Used to parse multiple files
    global DEF_IN
    i = 0

    #Used to clear data from previous operations
    f = open("videos.csv", "w")
    f1 = open("related.csv", "w")
    f.close()
    f1.close()

    while(True):
        try:
            args = sys.argv[1:]
            inp = Path(args[0]) if len(args) >= 1 else DEF_IN
            videos_out = Path(args[1]) if len(args) >= 2 else DEF_V
            related_out = Path(args[2]) if len(args) >= 3 else DEF_R

            if not inp.exists():
                raise FileNotFoundError(f"Input TSV not found: {inp}")

            tsv_to_neo_csv(inp, videos_out, related_out)
            print(f"Wrote: {videos_out.resolve()}")
            print(f"Wrote: {related_out.resolve()}")
        
        except FileNotFoundError:
            exit()

        i += 1
        DEF_IN = Path(f"{i}.tsv")

if __name__ == "__main__":
    main()
