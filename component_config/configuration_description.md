Transfers configs to projects defined in the table: 
`configs.csv` with cols (`["project_id","config_id", "component_id"]`)

Outputs table `transferred_configs_log` (`['project_id', 'region', 'src_cfg_id', 'dst_cfg_id', 'component_id', 'time']`)

**NOTE**: One configuration config is not transferred more than once,  
**EXCEPT** component of type `orchestrator`, which are transferred **each time**