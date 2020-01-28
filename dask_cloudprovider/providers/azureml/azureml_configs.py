class AzureMLConfigurations(object):
    azure_cpu_cms = {
          'STANDARD_D11_V2': 2
        , 'STANDARD_D12_V2': 4
        , 'STANDARD_D13_V2': 8
        , 'STANDARD_D14_V2': 16
        , 'STANDARD_D15_V2': 20
    }

    azure_gpu_vms = {
      'Standard_NC6s_v2'    : 1,
      'Standard_NC12s_v2'   : 2,
      'Standard_NC24s_v2'   : 4,
      'Standard_NC24sr_v2'  : 4,
      'Standard_NC6s_v3'    : 1,
      'Standard_NC12s_v3'   : 2,
      'Standard_NC24s_v3'   : 4,
      'Standard_NC24sr_v3'  : 4
    }