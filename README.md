An Object oriented, NoSQL, distributed, scalable database with lazy eventual consistency

This project tries to emulate what guys at Google and Amazon did with their DataStores or Dynamos.
Small fingerprint databases assuming read/write ratios of 10.000/1 or above where consistency is 
less important than scalability and data durability. Neither transactions nor ACID operations 
are expected. Just insert or remove objects from datastores.

- Rest API to get data from/inser data into the databases
- Rest API to synchronize datasets between database servers
- Many Master-2-Many Slaves architecture. Single Master, many slaves in first release