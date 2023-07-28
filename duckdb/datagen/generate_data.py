import duckdb

print(duckdb.__version__)

def main():
    con = duckdb.connect()

    num_columns = [10, 100, 1000, 10000, 100000, 1000000]

    for col in num_columns:
        columns = ""
        for i in range(col):
            if i == col - 1:
                columns += f"col_{i} bigint"
            else:
                columns += f"col_{i} bigint, "

        con.execute(
            f"""
            CREATE TABLE columns_{col}({columns})
            """)
        con.execute(
        f"""
        COPY (SELECT * FROM columns_{col}) TO 'columns_{col}.parquet' (FORMAT 'parquet')
        """)


if __name__ == "__main__":
    main()
