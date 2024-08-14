import json

def convert_json_type_to_avro_type(json_type):
    """Convert JSON schema type to Avro schema type."""
    mapping = {
        "string": "string",
        "integer": "int",
        "boolean": "boolean",
        "array": "array",
        "object": "record"
    }
    return mapping.get(json_type, "string")

def convert_properties_to_fields(properties):
    """Convert JSON schema properties to Avro fields."""
    fields = []
    for field_name, field_props in properties.items():
        field_type = convert_json_type_to_avro_type(field_props['type'])
        if field_type == "record":
            sub_fields = convert_properties_to_fields(field_props['properties'])
            fields.append({
                "name": field_name,
                "type": {
                    "type": "record",
                    "name": field_name.capitalize(),
                    "fields": sub_fields
                }
            })
        elif field_type == "array":
            items = field_props.get('items', {})
            if isinstance(items, dict):
                if 'type' in items:
                    item_type = convert_json_type_to_avro_type(items['type'])
                    if item_type == "record":
                        sub_fields = convert_properties_to_fields(items['properties'])
                        fields.append({
                            "name": field_name,
                            "type": {
                                "type": "array",
                                "items": {
                                    "type": "record",
                                    "name": field_name.capitalize() + "Item",
                                    "fields": sub_fields
                                }
                            }
                        })
                    else:
                        fields.append({
                            "name": field_name,
                            "type": {
                                "type": "array",
                                "items": item_type
                            }
                        })
                else:
                    fields.append({
                        "name": field_name,
                        "type": {
                            "type": "array",
                            "items": "string"  # Default to string if type is not specified
                        }
                    })
            else:
                fields.append({
                    "name": field_name,
                    "type": {
                        "type": "array",
                        "items": "string"  # Default to string if items is not a dict
                    }
                })
        else:
            fields.append({
                "name": field_name,
                "type": field_type
            })
    return fields

def json_schema_to_avro_schema(json_schema):
    """Convert JSON schema to Avro schema."""
    avro_schema = {
        "type": "record",
        "name": "Root",
        "fields": convert_properties_to_fields(json_schema['properties'])
    }
    return avro_schema

if __name__ == "__main__":
    json_schema = {
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "object",
  "properties": {
    "id": {
      "type": "string"
    },
    "version": {
      "type": "string"
    },
    "usecase": {
      "type": "string"
    },
    "reAwakable": {
      "type": "boolean"
    },
    "idPaymentManager": {
      "type": "string"
    },
    "complete": {
      "type": "boolean"
    },
    "receiptId": {
      "type": "string"
    },
    "businessProcess": {
      "type": "string"
    },
    "useCase": {
      "type": "string"
    },
    "missingInfo": {
      "type": "array",
      "items": {
        
      }
    },
    "debtorPosition": {
      "type": "object",
      "properties": {
        "modelType": {
          "type": "string"
        },
        "iuv": {
          "type": "string"
        },
        "iur": {
          "type": "string"
        },
        "noticeNumber": {
          "type": "string"
        }
      },
      "required": [
        "modelType",
        "iuv",
        "iur",
        "noticeNumber"
      ]
    },
    "creditor": {
      "type": "object",
      "properties": {
        "idPA": {
          "type": "string"
        },
        "idBrokerPA": {
          "type": "string"
        },
        "idStation": {
          "type": "string"
        },
        "companyName": {
          "type": "string"
        },
        "officeName": {
          "type": "string"
        }
      },
      "required": [
        "idPA",
        "idBrokerPA",
        "idStation",
        "companyName",
        "officeName"
      ]
    },
    "psp": {
      "type": "object",
      "properties": {
        "idPsp": {
          "type": "string"
        },
        "idBrokerPsp": {
          "type": "string"
        },
        "idChannel": {
          "type": "string"
        },
        "psp": {
          "type": "string"
        }
      },
      "required": [
        "idPsp",
        "idBrokerPsp",
        "idChannel",
        "psp"
      ]
    },
    "debtor": {
      "type": "object",
      "properties": {
        "fullName": {
          "type": "string"
        },
        "streetName": {
          "type": "string"
        },
        "civicNumber": {
          "type": "string"
        },
        "postalCode": {
          "type": "string"
        },
        "city": {
          "type": "string"
        },
        "stateProvinceRegion": {
          "type": "string"
        },
        "country": {
          "type": "string"
        },
        "entityUniqueIdentifierType": {
          "type": "string"
        },
        "entityUniqueIdentifierValue": {
          "type": "string"
        }
      },
      "required": [
        "fullName",
        "streetName",
        "civicNumber",
        "postalCode",
        "city",
        "stateProvinceRegion",
        "country",
        "entityUniqueIdentifierType",
        "entityUniqueIdentifierValue"
      ]
    },
    "payer": {
      "type": "object",
      "properties": {
        "fullName": {
          "type": "string"
        },
        "entityUniqueIdentifierType": {
          "type": "string"
        },
        "entityUniqueIdentifierValue": {
          "type": "string"
        }
      },
      "required": [
        "fullName",
        "entityUniqueIdentifierType",
        "entityUniqueIdentifierValue"
      ]
    },
    "paymentInfo": {
      "type": "object",
      "properties": {
        "paymentDateTime": {
          "type": "string"
        },
        "paymentToken": {
          "type": "string"
        },
        "applicationDate": {
          "type": "string"
        },
        "transferDate": {
          "type": "string"
        },
        "dueDate": {
          "type": "string"
        },
        "totalNotice": {
          "type": "string"
        },
        "touchpoint": {
          "type": "string"
        },
        "remittanceInformation": {
          "type": "string"
        },
        "amount": {
          "type": "string"
        },
        "fee": {
          "type": "string"
        },
        "paymentMethod": {
          "type": "string"
        },
        "IUR": {
          "type": "string"
        },
        "description": {
          "type": "string"
        },
        "metadata": {
          "type": "array",
          "items": [
            {
              "type": "object",
              "properties": {
                "key": {
                  "type": "string"
                },
                "value": {
                  "type": "string"
                }
              },
              "required": [
                "key",
                "value"
              ]
            }
          ]
        }
      },
      "required": [
        "paymentDateTime",
        "paymentToken",
        "applicationDate",
        "transferDate",
        "dueDate",
        "totalNotice",
        "touchpoint",
        "remittanceInformation",
        "amount",
        "fee",
        "paymentMethod",
        "IUR",
        "description",
        "metadata"
      ]
    },
    "transferList": {
      "type": "array",
      "items": [
        {
          "type": "object",
          "properties": {
            "idTransfer": {
              "type": "string"
            },
            "fiscalCodePA": {
              "type": "string"
            },
            "companyName": {
              "type": "string"
            },
            "amount": {
              "type": "string"
            },
            "transferCategory": {
              "type": "string"
            },
            "remittanceInformation": {
              "type": "string"
            },
            "IBAN": {
              "type": "string"
            },
            "metadata": {
              "type": "array",
              "items": [
                {
                  "type": "object",
                  "properties": {
                    "key": {
                      "type": "string"
                    },
                    "value": {
                      "type": "string"
                    }
                  },
                  "required": [
                    "key",
                    "value"
                  ]
                }
              ]
            },
            "MBD": {
              "type": "boolean"
            },
            "IUR": {
              "type": "string"
            }
          },
          "required": [
            "idTransfer",
            "fiscalCodePA",
            "companyName",
            "amount",
            "transferCategory",
            "remittanceInformation",
            "IBAN",
            "metadata",
            "MBD",
            "IUR"
          ]
        }
      ]
    },
    "transactionDetails": {
      "type": "object",
      "properties": {
        "info": {
          "type": "object",
          "properties": {
            "brand": {
              "type": "string"
            },
            "brandLogo": {
              "type": "string"
            },
            "clientId": {
              "type": "string"
            },
            "paymentMethodName": {
              "type": "string"
            },
            "type": {
              "type": "string"
            }
          },
          "required": [
            "brand",
            "brandLogo",
            "clientId",
            "paymentMethodName",
            "type"
          ]
        },
        "origin": {
          "type": "string"
        },
        "transaction": {
          "type": "object",
          "properties": {
            "idTransaction": {
              "type": "integer"
            },
            "grandTotal": {
              "type": "integer"
            },
            "amount": {
              "type": "integer"
            },
            "fee": {
              "type": "integer"
            },
            "paymentGateway": {
              "type": "string"
            },
            "transactionStatus": {
              "type": "string"
            },
            "accountingStatus": {
              "type": "string"
            },
            "rrn": {
              "type": "string"
            },
            "authorizationCode": {
              "type": "string"
            },
            "creationDate": {
              "type": "string"
            },
            "numAut": {
              "type": "string"
            },
            "accountCode": {
              "type": "string"
            },
            "psp": {
              "type": "object",
              "properties": {
                "brokerName": {
                  "type": "string"
                },
                "idChannel": {
                  "type": "string"
                },
                "businessName": {
                  "type": "string"
                },
                "serviceName": {
                  "type": "string"
                },
                "idPsp": {
                  "type": "string"
                },
                "pspOnUs": {
                  "type": "boolean"
                }
              },
              "required": [
                "brokerName",
                "idChannel",
                "businessName",
                "serviceName",
                "idPsp",
                "pspOnUs"
              ]
            },
            "timestampOperation": {
              "type": "string"
            },
            "transactionId": {
              "type": "string"
            },
            "errorCode": {
              "type": "string"
            }
          },
          "required": [
            "idTransaction",
            "grandTotal",
            "amount",
            "fee",
            "paymentGateway",
            "transactionStatus",
            "accountingStatus",
            "rrn",
            "authorizationCode",
            "creationDate",
            "numAut",
            "accountCode",
            "psp",
            "timestampOperation",
            "transactionId",
            "errorCode"
          ]
        },
        "wallet": {
          "type": "object",
          "properties": {
            "idWallet": {
              "type": "integer"
            },
            "walletType": {
              "type": "string"
            },
            "enableableFunctions": {
              "type": "array",
              "items": [
                {
                  "type": "string"
                },
                {
                  "type": "string"
                },
                {
                  "type": "string"
                }
              ]
            },
            "pagoPa": {
              "type": "boolean"
            },
            "onboardingChannel": {
              "type": "string"
            },
            "favourite": {
              "type": "boolean"
            },
            "createDate": {
              "type": "string"
            },
            "info": {
              "type": "object",
              "properties": {
                "type": {
                  "type": "string"
                },
                "holder": {
                  "type": "string"
                },
                "blurredNumber": {
                  "type": "string"
                },
                "hashPan": {
                  "type": "string"
                },
                "expireMonth": {
                  "type": "string"
                },
                "expireYear": {
                  "type": "string"
                },
                "brand": {
                  "type": "string"
                },
                "brandLogo": {
                  "type": "string"
                }
              },
              "required": [
                "type",
                "holder",
                "blurredNumber",
                "hashPan",
                "expireMonth",
                "expireYear",
                "brand",
                "brandLogo"
              ]
            }
          },
          "required": [
            "idWallet",
            "walletType",
            "enableableFunctions",
            "pagoPa",
            "onboardingChannel",
            "favourite",
            "createDate",
            "info"
          ]
        },
        "user": {
          "type": "object",
          "properties": {
            "idUser": {
              "type": "integer"
            },
            "userStatus": {
              "type": "integer"
            },
            "userStatusDescription": {
              "type": "string"
            },
            "type": {
              "type": "string"
            }
          },
          "required": [
            "idUser",
            "userStatus",
            "userStatusDescription",
            "type"
          ]
        }
      },
      "required": [
        "info",
        "origin",
        "transaction",
        "wallet",
        "user"
      ]
    },
    "properties": {
      "type": "object",
      "properties": {
        "serviceIdentifier": {
          "type": "string"
        }
      },
      "required": [
        "serviceIdentifier"
      ]
    },
    "timestamp": {
      "type": "integer"
    },
    "eventStatus": {
      "type": "string"
    },
    "eventRetryEnrichmentCount": {
      "type": "integer"
    },
    "_rid": {
      "type": "string"
    },
    "_self": {
      "type": "string"
    },
    "_etag": {
      "type": "string"
    },
    "_attachments": {
      "type": "string"
    },
    "_ts": {
      "type": "integer"
    }
  },
  "required": [
    "id",
    "version",
    "usecase",
    "reAwakable",
    "idPaymentManager",
    "complete",
    "receiptId",
    "businessProcess",
    "useCase",
    "missingInfo",
    "debtorPosition",
    "creditor",
    "psp",
    "debtor",
    "payer",
    "paymentInfo",
    "transferList",
    "transactionDetails",
    "properties",
    "timestamp",
    "eventStatus",
    "eventRetryEnrichmentCount",
    "_rid",
    "_self",
    "_etag",
    "_attachments",
    "_ts"
  ]
}
    
    avro_schema = json_schema_to_avro_schema(json_schema)
    print(json.dumps(avro_schema, indent=2))
