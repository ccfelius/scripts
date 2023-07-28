import duckdb

print(duckdb.__version__)


def generate_rows(cols):

    """ returns row with cols columns"""

    return ', '.join(cols * [str(12345678901)])

def main():
    con = duckdb.connect()

    num_columns = [100, 1000, 10000]

    for col in num_columns:
        columns = ""
        for i in range(col):
            if i == col - 1:
                columns += f"col_data_{i} bigint"
            else:
                columns += f"col_data_{i} bigint, "

        con.execute(
            f"""
            CREATE TABLE columns_{col}({columns})
            """)

        for i in range(100):
            con.execute(
            f"""
            INSERT INTO columns_{col} VALUES ({generate_rows(col)});
            """)

        con.execute(
        f"""
        COPY (SELECT * FROM columns_{col}) TO 'columns_{col}.parquet' (FORMAT 'parquet')
        """)

        print(f"File columns_{col} written")

if __name__ == "__main__":
    main()
