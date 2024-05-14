import json
from typing import DefaultDict


def load_json(filepath):
    with open(filepath, "r", encoding="utf-8") as file:
        return json.load(file)


def dump_json(filepath, data):
    with open(filepath, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)


def collect_by_columns(columns, rows):
    val_by_colum = DefaultDict(list)
    for row in rows:
        for ind, column in enumerate(columns):
            val_by_colum[column].append(row[ind])
    return val_by_colum


def eval_сopy(columns1, rows1, columns2, rows2):
    val_by_column1 = collect_by_columns(columns1, rows1)

    values_lists1 = [val_by_column1[column] for column in columns2]
    zipped_list1 = list(zip(*values_lists1))

    assert zipped_list1 == rows2


def eval_shuffle(columns1, rows1, columns2, rows2, groups, batch):
    val_by_column1 = collect_by_columns(columns1, rows1)
    val_by_column2 = collect_by_columns(columns2, rows2)

    for group in groups:
        values_lists1 = [val_by_column1[column] for column in group]
        values_lists2 = [val_by_column2[column] for column in group]

        zipped_list1 = list(zip(*values_lists1))
        zipped_list2 = list(zip(*values_lists2))

        for ind in range(0, len(zipped_list1), batch):
            assert (
                zipped_list1[ind : min(ind + batch, len(zipped_list1))]
                != zipped_list2[ind : min(ind + batch, len(zipped_list1))]
            )

        zipped_list1.sort()
        zipped_list2.sort()

        assert zipped_list1 == zipped_list2
