#### Step 5: Project Write Up
* _The rationale for the choice of tools and technologies for the project_. 
    - A data lake model through SPARK was chosen, because it facilitates the handling of large file types & sizes conveniently & efficiently. Both PySpark and SparkSQL were used to manipulate the data in dataframes and tables. 

* _How often the data should be updated and why?_ 
    - The data should be updated monthly to reflect the current partition of data & current aggregation in tables

* _How should we would approach the problem differently under the following scenarios_:
    * **The data was increased by 100x**. First of all, the raw data should be migrated in the clouds, e.g., in an `S3` bucket. Then, the ELT process can be facilitated with SPARK using an Amazon Redshift cluster (_see figure below_). 

    <img src="../images/remote.png" alt="project" title="local" width="520" align="center" class="center" style="padding-left: 25px" /> <br />



    * The data populates a dashboard that **must be updated on a daily basis by 7am every day**. A scheduling tool, e.g., Airflow, should be used to update the tables with 'fresh' data every night.

     * The database **needed to be accessed by 100+ people**. Hosting the data on the clouds would be most appropriate, as these can be accessed individually, e.g., via EMR or Redshift.