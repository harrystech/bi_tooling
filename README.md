# Looker Tools

## Setup

In order to run this script you'll need to generate the Looker module used to hit the Looker API as well as set some environment variables.

### Looker API SDK

Create the Swagger generated Python SDK following the instructions [here](https://discourse.looker.com/t/generating-client-sdks-for-the-looker-api/3185). That code needs to be saved in the same directory as the `verify_lookml.py` script so that the module can be directly imported.

Once you generate the Python SDK, you need to adjust the `PoolManager` in `rest.py` to allow for multiple connections: 

```python
        # https pool manager
        self.pool_manager = urllib3.PoolManager(
            num_pools=pools_size,
            cert_reqs=cert_reqs,
            ca_certs=ca_certs,
            cert_file=cert_file,
            maxsize=16,  # This needs to be manually set
            key_file=key_file
        )
```

### Environment Variables

This script requires three environment variables to be set:
```
export LOOKER_BASE_URL=https://<example>.looker.com:19999/api/3.0/
export LOOKER_API_ID=<your-id-here>
export LOOKER_API_SECRET=<your-secret-here>
```


## Getting Started

```bash
python verify_lookml.py distribution

2018-03-02 16:52:58,960 Loading 'distribution' model
2018-03-02 16:52:58,960 Loading 'distribution_reconciliation' explore
2018-03-02 16:52:59,596 Loading 'distribution' explore
2018-03-02 16:53:00,313 Loading 'shipping_status' explore
2018-03-02 16:53:01,134 Loading 'shipments' explore
2018-03-02 16:53:02,021 Loading 'SKU_Costs' explore
2018-03-02 16:53:19,392 All fields in queue have been processed
2018-03-02 16:53:19,392 No Errors detected in 'distribution_reconciliation' explore
2018-03-02 16:53:19,392 No Errors detected in 'SKU_Costs' explore
2018-03-02 16:53:19,392 No Errors detected in 'distribution' explore
2018-03-02 16:53:20,394 Error field: 'order_attributes.test_broken_dim' explore :'shipments'
2018-03-02 16:53:20,394 Error field: 'order_attributes.test_broken_dim' explore :'shipping_status'
2018-03-02 16:53:21,395 Total time to completion: 23.176 seconds
```

This script can exectute one or many models at once. To validate a single model, enter:

```python
python verify_lookml.py [model-name]
```
If no model is passed in, the the script will check all models.
