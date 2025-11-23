import os
import csv
import sys
from pathlib import Path

# defaults:
DEF_IN = Path("0.tsv")
DEF_V = Path("outputs/videos.csv")
DEF_R = Path("outputs/related.csv")

#tracks how many times a 0.tsv has been accounted for
header_written = False

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

        global header_written

        vwriter = csv.writer(fv, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
        rwriter = csv.writer(fr, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")

        if not header_written:
            vwriter.writerow(["videoId","uploader","age","category","length","views","rating","ratingCount","commentCount"])
            rwriter.writerow(["srcVideoId","dstVideoId"])
            header_written = True

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

def main(videos, related):

    #Used to parse multiple files
    input_dir = os.getcwd() + "\\input"
    global DEF_IN

    #Used to clear data from previous operations
    f = open(Path(videos), "w")
    f1 = open(Path(related), "w")
    f.close()
    f1.close()

    for file in os.listdir(input_dir):
        file_path = os.path.join(input_dir, file)
        file_path = Path(file_path)

        if os.path.isfile(file_path):
            try:
                args = sys.argv[1:]
                inp = DEF_IN
                videos_out = Path(videos)
                related_out = Path(related)

                if not file_path.exists():
                    raise FileNotFoundError(f"Input TSV not found: {inp}")

                tsv_to_neo_csv(file_path, videos_out, related_out)
                print(f"Wrote: {videos_out.resolve()}")
                print(f"Wrote: {related_out.resolve()}")
            
            except FileNotFoundError:
                print(f"Could not find file path {file_path}")
                exit()


if __name__ == "__main__":
    main()
