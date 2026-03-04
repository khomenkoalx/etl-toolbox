from typing import List

import pandas as pd
from core.crud import mapper
from core.dadata_client import get_region_id_by_dadata
from core.constants import ID_CLIENT_MISMATCH_MESSAGE
from core.smtp_client import send_notification


def to_int_or_none(x):
    if pd.isna(x) or x is None or str(x).strip() in ['~', '']:
        return None
    try:
        return int(str(x).strip())
    except:
        return x


class RegionTransformer:

    def get_region_id(self, region_name):
        if pd.isna(region_name):
            return None

        region_str = str(region_name).strip()
        region_id = mapper.get_or_create_id_region(region_str)

        if not region_id:
            region_id = get_region_id_by_dadata(region_str)
            if region_id:
                print(f'Найден id для региона {region_str} - {region_id}')
                mapper.update_id_of_region(region_str, region_id)

        return region_id

    def process_region(
            self,
            df: pd.DataFrame,
            input_columns: List[str],
            output_column: str, **kwargs
    ):
        region_col = input_columns[0]
        result_df = df.copy()
        result_df[output_column] = result_df[region_col].apply(self.get_region_id)
        return result_df


class ClientTransformer:

    def get_client_and_id(self, client_tuple):
        return (client_tuple[0] or client_tuple[1], client_tuple[2])

    def prepare_client_tuples(self, df, input_columns):
        client_name_col, client_detail_col = input_columns[0], input_columns[1]
        client_id_col = input_columns[2] if len(input_columns) > 2 else None

        cols = [client_name_col, client_detail_col]
        if client_id_col and client_id_col in df.columns:
            cols.append(client_id_col)

        unique_data = df[cols].drop_duplicates()

        client_tuples = []
        for _, row in unique_data.iterrows():
            client_tuple = (
                row[client_name_col],
                row[client_detail_col],
                row[client_id_col] if client_id_col in row else None
            )
            client_tuples.append(client_tuple)

        return client_tuples

    def process_client_tuple(self, client_tuple):
        client_tuple_clean = (
            to_int_or_none(client_tuple[0]),
            to_int_or_none(client_tuple[1]),
            to_int_or_none(client_tuple[2])
        )

        client, id = self.get_client_and_id(client_tuple_clean)

        if not client:
            client = '~'

        client = str(client)
        id_in_db = mapper.get_or_create_id_client(client, id)

        mismatch = None
        if id and id_in_db != id and id_in_db != 10003:
            mismatch = f'Несовпадение, id_in_db: {id_in_db}, id: {id}, клиент: {client}'

        key = (client_tuple_clean[0], client_tuple_clean[1])
        return key, id_in_db, mismatch

    def process_clients(self, df, input_columns, output_column, **kwargs):
        client_tuples = self.prepare_client_tuples(df, input_columns)

        mismatches = []
        client_mapping = {}

        for client_tuple in client_tuples:
            key, id_in_db, mismatch = self.process_client_tuple(client_tuple)

            if mismatch:
                mismatches.append(mismatch)

            client_mapping[key] = id_in_db

        if mismatches:
            send_notification(
                text=ID_CLIENT_MISMATCH_MESSAGE.format('\n'.join(mismatches)),
                subject='Несовпадения ID клиентов в базе и данных RNC'
            )

        result_df = df.copy()
        client_name_col, client_detail_col = input_columns[0], input_columns[1]

        def get_client_id(row):
            key = (
                to_int_or_none(row[client_name_col]),
                to_int_or_none(row[client_detail_col])
            )
            return client_mapping.get(key)

        result_df[output_column] = result_df.apply(get_client_id, axis=1)
        return result_df
