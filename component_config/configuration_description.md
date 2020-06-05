Generates projects of specified types for users defined in table: `users.csv` with cols (`["email","name"]`)

Outputs table `transferred_configs_log` (`['project_id', 'region', 'src_cfg_id', 'dst_cfg_id', 'component_id', 'time']`)

**NOTE**: One configuration config is not transferred more than once,  
**EXCEPT** component of type `orchestrator`, which are transferred **each time**