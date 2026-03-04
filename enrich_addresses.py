import pandas as pd

from core.models import DimAddress
from core.dadata_client import get_address_info_by_fias
from core.crud import address_crud

#df = pd.read_csv(r'C:\Users\ahomenko\Workplace\dev\rnc_processor\addresess_data\АДРЕСА.csv', encoding='cp1251', sep='\t')
#df = df[~df['fias_code_vis_ru'].isin(['НЕ ОПРЕДЕЛЕНО', '~', 'ТЕРРИТОРИЯ ВНЕ КЛАССИФИКАТОРА'])]
#unique_id_fias = df['fias_code_vis_ru'].unique()


#for id_fias in unique_id_fias:
#    address_crud.add_new_fias(id_fias)

#get_address_info_by_fias('1ab5aaa5-b659-42da-a433-42395d27ecf7')
#address_to_fill = address_crud.get_address_by_fias('b4da1234-5678-4abc-9def-0123456789ab')
#address_to_fill_id_fias = address_to_fill.id_fias

#address_crud.add_new_fias('1ab5aaa5-b659-42da-a433-42495d27ecf7')

if __name__ == '__main__':

    entities_with_missing_data = address_crud.get_addresses_with_missing_data()

    for index, entity in enumerate(entities_with_missing_data):
        address_info = get_address_info_by_fias(str(entity.id_fias))
        address_crud.update_address(entity.id_fias, **address_info)
