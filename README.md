# KBC Component

Transfers configs to projects defined in the table: 
`configs.csv` with cols (`["project_id","configuration_id", "component_id"]`)


Outputs table `transferred_configs_log` (`['project_id', 'region', 'src_cfg_id', 'dst_cfg_id', 'component_id', 'time']`)

**Table of contents:**  
  
[TOC]


# Functionality notes

Component type `flow` will look for component `keboola.orchestrator`

**NOTE**: One configuration config is not transferred more than once,  

**EXCEPT** component of type `orchestrator-legacy`, which are transferred **each time**

# Configuration

## Param 1

## Param 2


## Development

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to your custom path in the docker-compose file:

```yaml
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
```

Clone this repository, init the workspace and run the component with following command:

```
git clone repo_path my-new-component
cd my-new-component
docker-compose build
docker-compose run --rm dev
```

Run the test suite and lint check using this command:

```
docker-compose run --rm test
```

# Integration

For information about deployment and integration with KBC, please refer to the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/) 