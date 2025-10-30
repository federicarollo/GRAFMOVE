# Open Data and Air Quality Data for Personalized Route Planning in Smart Cities
This repository contains the materials and code used for the demo presented at the **4th Italian Conference on Big Data and Data Science** (ITADATA), held in Turin (Italy) on September 9, 2025. 
It includes slides, source code, datasets, and examples to support the session.


This tutorial explores how open data can reshape urban mobility by enabling personalized, health-conscious routing for pedestrians and cyclists.

The source code (Python code in the [code folder](https://github.com/federicarollo/ITADATA25/tree/main/code)) provides an implementation of multi-objective optimization in route planning showing examples from real-world use cases in Modena and Ferrara through several steps:
- **Step 1 - Graph Import**
	- Import and explore graphs generated from OpenStreetMap using Neo4j where contextual data (e.g., air pollution, road accidents) have been integrated
- **Step 2 - Path Generation**
	- Define a start point and an end point (from a set of suggested points)
	- Generate a sufficiently large set of candidate paths between the two points for multi-criteria analysis
- **Step 3 - Pareto Front Identification**
	- Identify the Pareto-optimal set (non-dominated solutions) among candidate paths
- **Step 4 - Optimal Path Selection and Interactive Exploration**
	- Select the most suitable path from the Pareto front based on user preferences
	- Visualize the selected path on the map
	- Modify the user preferences and explore how they affect the optimal solution

The code can be run through [Google Colab](colab.research.google.com) or a Python IDE (e.g., Jupyter notebook).



For further details, you may refer to and cite the following publications:

<pre>Federica Rollo and Laura Po. 2025. 
<i>GRAFMOVE: Graph-based Mobility Optimization and Visualization Engine.</i>
In Proceedings of the 33rd ACM International Conference on Advances in 
Geographic Information Systems (Minneapolis, MN, USA) (SIGSPATIAL ’25). 
Association for Computing Machinery, New York, NY, USA. 
doi:10.1145/3748636.3762798 </pre>

<pre>Federica Rollo and Laura Po. 2025. 
<i>MODyPer: Multi-Objective Dynamic Personalized Route Planning for Vulnerable Road Users.</i>
In Proceedings of the 33rd ACM International Conference on Advances in 
Geographic Information Systems (Minneapolis, MN, USA) (SIGSPATIAL ’25). 
Association for Computing Machinery, New York, NY, USA. 
doi:10.1145/3748636.3764160 </pre>



*This work was carried out as part of the AIQS project (AI-enhanced Air Quality Sensor for Optimizing Green Routes), under the project code DIP_AIQS_PO_2025_PNRR_ECOS_SK4AF_E93C22001100001. AIQS was funded through a closed call within the initiative ”Ecosystem for Sustainable Transition in Emilia-Romagna” (ECOSISTER), financed under the National Recovery and Resilience Plan (PNRR) – Mission 4 “Education and Research”, Component 2 “From Research to Business”, Investment 1.5 “Creation and strengthening of innovation ecosystems, building territorial R&D leaders” – funded by the European Union – NextGenerationEU (Grant Agreement No. 0001052, dated 23/06/2022 – Project ECS 00000033 – CUP E93C22001100001)".*
