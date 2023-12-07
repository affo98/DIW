import pandas as pd
import numpy as np
import pyarrow
from helper_functions import tokenize_text


def main():
    data_path = "./data/"
    data_speech1 = pd.read_parquet(data_path + "data_speech1.parquet")
    data_speech2 = pd.read_parquet(data_path + "data_speech2.parquet")
    data_speech3 = pd.read_parquet(data_path + "data_speech3.parquet")

    data_speech1 = tokenize_text(data_speech1)
    data_speech3 = tokenize_text(data_speech2)
    data_speech3 = tokenize_text(data_speech3)

    dspeech = pd.concat([data_speech1, data_speech2, data_speech3], axis=0)
    dspeech = dspeech[
        [
            "meeting_id",
            "agenda_item_id",
            "speech_item_id",
            "label_agenda",
            "speech_item_tokenized",
        ]
    ]
    dspeech.to_parquet(
        data_path + "data_speech_tok.parquet", index=False, engine="Pyarrow"
    )


if __name__ == "__main__":
    main()
