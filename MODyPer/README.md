# MODyPer: Multi-Objective Dynamic Personalized Route Planning for Vulnerable Road Users

MODyPer integrates heterogeneous spatial data into a graph and identifies optimal routes based on dynamic user preferences across multiple objectives.

The code used to generate the graph is available in the [GRAFMOVE repository](https://github.com/federicarollo/GRAFMOVE/).

This repository provides the implementation of the multi-objective optimization algorithm for route planning, extracting data from the graph.

- **Step 1 - Graph Creation**
- **Step 2 - Candidate Paths Generation**
	- Define a start point and an end point
	- Generate a sufficiently large set of candidate paths between the two points for multi-criteria analysis
- **Step 3 - Pareto Front Identification**
	- Identify the Pareto-optimal set (non-dominated solutions) among candidate paths
- **Step 4 - Optimal Path Selection and Interactive Exploration**
	- Select the most suitable path from the Pareto front based on user preferences
	- Visualize the path on the map

