# GRAFMOVE: Graph-based Mobility Optimization and Visualization Engine

The json file can be loaded in NeoDash to create a new dashboard connected to a new graph.

Before loading the dashboard, please execute the following Cypher queries on the graph:


`call gds.graph.project('graph_component', 'FootNode', 'ROUTE')`


`CALL gds.wcc.write('graph_component', { writeProperty: 'componentId' })
YIELD nodePropertiesWritten, componentCount;`


