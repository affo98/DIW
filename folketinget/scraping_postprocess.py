import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np


def postprocess_meeting_data():
    data_meeting = pd.read_csv("data_meeting.csv")
    # remove empty rows
    data_meeting = data_meeting[pd.notna(data_meeting["date"])]

    # Adding date, day and start time
    date_list = data_meeting["date"].astype(str).str.split(" ")
    data_meeting["date"] = date_list.apply(lambda x: " ".join(x[2:5]))
    month_mapping = {
        "januar": "January",
        "februar": "February",
        "marts": "March",
        "april": "April",
        "maj": "May",
        "juni": "June",
        "juli": "July",
        "august": "August",
        "september": "September",
        "oktober": "October",
        "november": "November",
        "december": "December",
    }
    data_meeting["date"] = (
        data_meeting["date"]
        .str.replace(".", "")
        .str.split(" ")
        .apply(lambda x: f"{x[0]} {month_mapping[x[1]]} {x[2]}")
    )
    data_meeting["date"] = pd.to_datetime(data_meeting["date"])
    data_meeting["day"] = date_list.str.get(0)
    data_meeting["time_start"] = date_list.str.get(-1)

    data_meeting = data_meeting.reset_index(drop=True)

    # save as parquet
    data_meeting_pq = pa.Table.from_pandas(data_meeting)
    pq.write_table(data_meeting_pq, "./data/data_meeting.parquet")


def postprocess_speech_data():
    data_speech = pd.read_csv("data_speech.csv")
    # remove empty rows
    data_speech = data_speech[pd.notna(data_speech["speech_item_id"])]

    # Put into correct format
    data_speech[["agenda_item_id", "speech_item_id"]] = data_speech[
        ["agenda_item_id", "speech_item_id"]
    ].astype(int)
    data_speech["time_start"] = data_speech["time_start"].str.split("T").str[1]
    data_speech["time_end"] = data_speech["time_end"].str.split("T").str[1]

    # remove rows with missing values
    data_speech = data_speech[
        pd.notna(data_speech["speaker_title"])
    ]  # 761 rows missing
    data_speech = data_speech[pd.notna(data_speech["speaker_role"])]  # 391 rows missing
    data_speech = data_speech[
        pd.notna(data_speech["speech_item_text"])
    ]  # 57 rows missing

    # remove rows where the moderator ('formand') is speaking
    data_speech = data_speech[
        (data_speech["speaker_role"] != "formand")
        & (data_speech["speaker_role"] != "aldersformanden")
        & (data_speech["speaker_role"] != "midlertidig formand")
        & (data_speech["speaker_role"] != "MødeSlut")
    ]

    # remove rows that is pause
    data_speech = data_speech[data_speech["speaker_party"] != "Pause"]

    # Fix missing values in 'speaker_party' when 'speaker_role' is 'medlem'
    data_speech.loc[
        (data_speech["speaker_party"].isnull())
        & (data_speech["speaker_role"] == "medlem"),
        "speaker_party",
    ] = data_speech.loc[
        (data_speech["speaker_party"].isnull())
        & (data_speech["speaker_role"] == "medlem"),
        "speaker_title",
    ].str.extract(
        r"\((.*?)\)"
    )[
        0
    ]

    # add duration column
    data_speech["time_start_f"] = pd.to_datetime(
        data_speech["time_start"], format="%H:%M:%S"
    )
    data_speech["time_end_f"] = pd.to_datetime(
        data_speech["time_end"], format="%H:%M:%S"
    )

    data_speech["duration"] = np.where(
        # if it crosses midnight
        data_speech["time_start_f"].dt.hour > data_speech["time_end_f"].dt.hour,
        (
            pd.to_datetime("23:59:59", format="%H:%M:%S") - data_speech["time_start_f"]
        ).dt.total_seconds()
        + (
            data_speech["time_end_f"] - pd.to_datetime("00:00:00", format="%H:%M:%S")
        ).dt.total_seconds(),
        # else
        (data_speech["time_end_f"] - data_speech["time_start_f"]).dt.total_seconds(),
    )

    data_speech.drop(columns=["time_start_f", "time_end_f"], inplace=True)

    # add number of words
    data_speech["number_of_words"] = (
        data_speech["speech_item_text"].str.split(" ").apply(len)
    )

    data_speech = data_speech.reset_index(drop=True)

    # split into three datasets and save
    split_point_1 = len(data_speech) // 3
    split_point_2 = 2 * (len(data_speech) // 3)

    data_speech_1 = pa.Table.from_pandas(data_speech.iloc[:split_point_1])
    data_speech_2 = pa.Table.from_pandas(data_speech.iloc[split_point_1:split_point_2])
    data_speech_3 = pa.Table.from_pandas(data_speech.iloc[split_point_2:])

    pq.write_table(data_speech_1, "./data/data_speech1.parquet")
    pq.write_table(data_speech_2, "./data/data_speech2.parquet")
    pq.write_table(data_speech_3, "./data/data_speech3.parquet")

    return data_speech


def postprocess_agenda_data(data_speech):
    data_agenda = pd.read_csv("data_agenda.csv")
    # Data Agenda
    data_agenda = data_agenda[pd.notna(data_agenda["type"])]
    data_agenda[["agenda_item_id", "type"]] = data_agenda[
        ["agenda_item_id", "type"]
    ].astype(int)
    data_speech["speech_item_text"] = data_speech["speech_item_text"].astype(str)

    # Make variables from speech_data
    data_speech_group = data_speech.groupby(["meeting_id", "agenda_item_id"])
    # Add speech items text
    data_speech_group_text = (
        data_speech_group["speech_item_text"].apply(lambda x: " ".join(x)).reset_index()
    )
    data_agenda = data_agenda.merge(
        data_speech_group_text, on=["meeting_id", "agenda_item_id"], how="left"
    )
    # Add time_start and time_end from speech_data
    data_speech_group_time_start = data_speech_group["time_start"].first().reset_index()
    data_speech_group_time_end = data_speech_group["time_end"].last().reset_index()
    data_agenda = data_agenda.merge(
        data_speech_group_time_start, on=["meeting_id", "agenda_item_id"], how="left"
    )
    data_agenda = data_agenda.merge(
        data_speech_group_time_end, on=["meeting_id", "agenda_item_id"], how="left"
    )

    # add duration column
    data_agenda["time_start_f"] = pd.to_datetime(
        data_agenda["time_start"], format="%H:%M:%S"
    )
    data_agenda["time_end_f"] = pd.to_datetime(
        data_agenda["time_end"], format="%H:%M:%S"
    )

    data_agenda["duration"] = np.where(
        # if it crosses midnight
        data_agenda["time_start_f"].dt.hour > data_agenda["time_end_f"].dt.hour,
        (
            pd.to_datetime("23:59:59", format="%H:%M:%S") - data_agenda["time_start_f"]
        ).dt.total_seconds()
        + (
            data_agenda["time_end_f"] - pd.to_datetime("00:00:00", format="%H:%M:%S")
        ).dt.total_seconds(),
        # else
        (data_agenda["time_end_f"] - data_agenda["time_start_f"]).dt.total_seconds(),
    )
    data_agenda.drop(columns=["time_start_f", "time_end_f"], inplace=True)

    # remove agenda items with missing text and reset index
    data_agenda = data_agenda[pd.notna(data_agenda["speech_item_text"])]
    data_agenda = data_agenda.reset_index(drop=True)

    # add number of words
    data_agenda["number_of_words"] = (
        data_agenda["speech_item_text"].str.split(" ").apply(len)
    )

    # split intwo two datasets and save
    split_point_1 = len(data_agenda) // 3
    split_point_2 = 2 * (len(data_agenda) // 3)

    data_agenda_1 = pa.Table.from_pandas(data_agenda.iloc[:split_point_1])
    data_agenda_2 = pa.Table.from_pandas(data_agenda.iloc[split_point_1:split_point_2])
    data_agenda_3 = pa.Table.from_pandas(data_agenda.iloc[split_point_2:])

    # Save the three datasets
    pq.write_table(data_agenda_1, "./data/data_agenda1.parquet")
    pq.write_table(data_agenda_2, "./data/data_agenda2.parquet")
    pq.write_table(data_agenda_3, "./data/data_agenda3.parquet")


postprocess_meeting_data()
data_speech_processed = postprocess_speech_data()
postprocess_agenda_data(data_speech_processed)
