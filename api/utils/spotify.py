from datetime import datetime

import ijson
import pandas as pd


def yield_spotify_entry():
    start_maps = 0
    with open("./assets/Streaming_History.json") as file:
        parser = ijson.parse(file)

        spotify_entry = {}
        for prefix, event, value in parser:
            if event == "start_map":
                spotify_entry.clear()

                start_maps += 1

            split_prefix = prefix.split(".")

            if len(split_prefix) == 2:
                key = prefix.split(".")[1]
                spotify_entry[key] = value
            if event == "end_map":
                date_obj = datetime.strptime(spotify_entry["ts"], "%Y-%m-%dT%H:%M:%SZ")
                start_yr = datetime(2022, 1, 1)
                end_yr = datetime(2023, 1, 1)

                if end_yr > date_obj > start_yr:
                    yield spotify_entry

    return start_maps


if __name__ == "__main__":
    print("heh")
    entry_generator = yield_spotify_entry()

    df = pd.DataFrame()

    for entry in entry_generator:
        if df.empty:
            df = pd.DataFrame([entry])
        else:
            new_entry = pd.DataFrame([entry])

            df = pd.concat([df, new_entry], join="inner")

    song_play_counts_df = (
        df.groupby(["master_metadata_track_name"]).size().reset_index(name="play_count")
    )

    sorted = song_play_counts_df.sort_values('play_count',ascending=False)


    print(sorted.head(40))
