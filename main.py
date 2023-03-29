import requests
import urllib3 # for disable SSL warnings

# API URLs list
BANKS = {
  "BB_API_URL": "https://opendata.api.bb.com.br",
  "BRADESCO_API_URL": "https://api.bradesco.com/bradesco",
  "CAIXA_API_URL": "https://api.openbanking.caixa.gov.br",
  "ITAU_API_URL": "https://api.itau",
  "PAN_API_URL": "https://api-openbanking.bancopan.com.br",
  "SANTANDER_API_URL": "https://openbanking.api.santander.com.br",
  "SICOOB_API_URL": "https://openfinance.sicoob.com.br",
  "SICREDI_API_URL": "https://api.sicredi.com.br"
}


def get_personal_credit_cards_data():
  endpoint = "/open-banking/products-services/v1/personal-credit-cards"
  for bank, host_url in BANKS.items():
    url = host_url + endpoint
    response = requests.get(url, verify=False)
    response_json = response.json()

    if response.status_code != 200:
      print(f"Error: {bank}")
      continue

    print(f"Bank: {bank}")

    while True:
      for company in response_json["data"]["brand"]["companies"]:
        for card in company["personalCreditCards"]:
          print(f"{company['name']} - {card['name']}:")
          for service in card["fees"]["services"]:
            print(f"  {service['name']}:")
            print(f"    minimum: {service['minimum']['value']}")
            for price in service["prices"]:
              print(f"    {price['interval']}: {price['value']}")
            print(f"    maximum: {service['minimum']['value']}")

      if "next" not in response_json["links"] or response_json["links"]["next"] == "" or response_json["links"]["next"] is None:
        break
      else:
        query_string = response_json["links"]["next"].split("?")[1]
        response_json = requests.get(url=url + '?' + query_string, verify=False).json()


def main():
  urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) # disable SSL warnings
  get_personal_credit_cards_data()


if __name__ == "__main__":
  main()
