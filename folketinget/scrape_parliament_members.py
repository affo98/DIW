import requests
from bs4 import BeautifulSoup
import pandas as pd
import warnings
import pyarrow as pa
import pyarrow.parquet as pq

warnings.simplefilter("ignore", category=FutureWarning)

parliaments_wiki = [
    "https://da.wikipedia.org/wiki/Folketingsmedlemmer_valgt_i_" + year
    for year in ["2005", "2007", "2011", "2015", "2019", "2022"]
]


def check_list_lengths(list):
    # All lists need to be multiple of 179. Because there are 179 members of the parliament.
    for j in range(len(all_texts)):
        print(f"Length of list {j}: {len(all_texts[j])}")
    c = 0
    r = 0
    for j in range(len(all_texts)):
        if len(all_texts[j]) % 179 != 0:
            print(f"List number {j} is not multiple of 179")
            c += 1
        if len(all_texts[j]) == 358:
            r += 1
    if c == 0:
        print("All lists are multiples of 179")
    if r == 6:
        print("All lists have correct length 358")


all_texts = []
for url in parliaments_wiki:
    page = requests.get(url)
    page = BeautifulSoup(page.text, "html.parser")
    h2_ids = [
        h2.find_next("span", {"class": "mw-headline"}).get("id")
        for h2 in page.find_all("h2")
    ]

    if url == "https://da.wikipedia.org/wiki/Folketingsmedlemmer_valgt_i_2007":
        h2_ids.pop(0)

    for h2_id in h2_ids:
        if h2_id in [
            "Valgte_folketingsmedlemmer",
            "Valgte_medlemmer_ved_valget_13.11.2007",
            "Liste_over_medlemmerne",
            "Valgte_folketingsmedlemmer_ved_valget_18.6.2015",
            "Valgte_folketingsmedlemmer_ved_valget_5._juni_2019",
            "Valgte_1._november_2022",
        ]:
            break
    h2 = page.find("span", {"class": "mw-headline", "id": h2_id})
    table = h2.find_next("table")
    texts = [a.text.strip() for a in table.find_all("td")]
    all_texts.append(texts)

print("\nLength of list before processing")
check_list_lengths(all_texts)

# Manually clean a few things to get the lists into the same format of multiple of 179
all_texts[0].insert(all_texts[0].index("Thomas Adelskov") + 1, "")
all_texts[5] = all_texts[5][:-1]

print("\nLength of list after processing")
check_list_lengths(all_texts)

# Get the elements from each list that we are interrested in: Name and political party
for i in range(len(all_texts)):
    if len(all_texts[i]) == 716:
        all_texts[i] = [all_texts[i][j] for j in range(0, len(all_texts[i]), 2)]

    if len(all_texts[i]) == 537:
        all_texts[i] = [all_texts[i][j] for j in range(len(all_texts[i])) if j % 3 != 2]

    if len(all_texts[i]) == 1074:
        all_texts[i] = [all_texts[i][j] for j in range(0, len(all_texts[i]), 3)]

    if len(all_texts[i]) == 895:
        all_texts[i] = [
            all_texts[i][j] for j in range(len(all_texts[i])) if j % 5 in (0, 1)
        ]

print("\nLength of list after selecting name and party")
check_list_lengths(all_texts)

# periods manually entered from 'parliaments_wiki'
# Last date of periods end is the date of the last meeting in our dataset
parliament_members = pd.DataFrame(
    columns=["speaker_name", "speaker_party", "period_start", "period_end"]
)
periods_start = [
    "2005-02-08",
    "2007-11-13",
    "2011-09-15",
    "2015-06-18",
    "2019-06-05",
    "2022-11-01",
]
periods_end = [
    "2007-11-13",
    "2011-09-15",
    "2015-06-18",
    "2019-06-05",
    "2022-11-01",
    "2023-09-07",
]

for i in range(len(all_texts)):
    data = {
        "speaker_name": [all_texts[i][j] for j in range(0, len(all_texts[i]), 2)],
        "speaker_party": [all_texts[i][j] for j in range(1, len(all_texts[i]), 2)],
        "period_start": periods_start[i],
        "period_end": periods_end[i],
    }

    parliament_members = parliament_members.append(
        pd.DataFrame(data), ignore_index=True
    )
parliament_members = pa.Table.from_pandas(parliament_members)
pq.write_table(parliament_members, "./data/parliament_members.parquet")

if parliament_members.shape[0] == 179 * 6:  # 6 parliaments scraped
    print("\nSuccessfully saved parliament_members.parquet")
