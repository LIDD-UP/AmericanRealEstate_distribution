



source_str = 'https://mapi-ng.rdc.moveaws.com/api/v1/properties?offset=0&limit=150&county=New+York&state_code=NY&sort=completeness%2Cphotos%2Cfreshest&schema=mapsearch&prop_status=for_rent&client_id=rdc_mobile_native%2C9.4.2%2Candroid'

import re
print(re.findall(r'for_rent',source_str)
      )

