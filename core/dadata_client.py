from dadata import Dadata

from core.config import settings

token = settings.dadata_token

dadata_client = Dadata(token)


def get_region_id_by_dadata(raw_region):
    result = dadata_client.suggest('address', raw_region)
    if result and result[0].get('data'):
        return int(result[0]['data']['region_kladr_id'][0:2])


def get_address_info_by_fias(id_fias):
    response = dadata_client.find_by_id('fias', id_fias)
    if not response:
        response = dadata_client.find_by_id('address', id_fias)
    result_dict = {'address_name': None, 'id_region': None, 'city': None}
    if response:
        result_dict['address_name'] = response[0]['value'][:256]
        result_dict['id_region'] = int(response[0]['data']['region_kladr_id'][:2])
        result_dict['city'] = response[0]['data']['city'] or response[0]['data']['settlement']
    return result_dict
