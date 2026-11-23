import csv

from scripts import generate_data


def test_write_csv_escapes_delimiters_and_quotes(tmp_path):
    path = tmp_path / "nested" / "sample.csv"

    generate_data.write_csv(
        path,
        ["name", "note"],
        [('Store, "Central"', "two\nlines")],
    )

    with path.open(newline="", encoding="utf-8") as file:
        rows = list(csv.reader(file))

    assert rows == [
        ["name", "note"],
        ['Store, "Central"', "two\nlines"],
    ]


def test_main_generates_complete_batch_fixtures(tmp_path, monkeypatch):
    monkeypatch.setattr(generate_data, "OUT", tmp_path)

    generate_data.main()

    for day in generate_data.DATES:
        batch_dir = tmp_path / f"batch_{day}"
        assert (batch_dir / ".COMPLETE").is_file()

        for source in (
            "pos_sales",
            "inventory_snapshot",
            "item_master",
            "store_master",
        ):
            path = batch_dir / f"{source}_{day}.csv"
            assert path.is_file()
            with path.open(newline="", encoding="utf-8") as file:
                rows = list(csv.reader(file))
            assert len(rows) > 1

    seed = tmp_path / "customer_seed.csv"
    assert seed.is_file()
    with seed.open(newline="", encoding="utf-8") as file:
        assert len(list(csv.reader(file))) == 3
