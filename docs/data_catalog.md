H1 Data Dictionary for Gold Layer

**Overview**
The Gold layer is the business level data representation, structured to support analytical ans reporting use case. it consist of dimension tables and fact tables for specific business metrics.

1. **gold.dim_customers
   **.** **Purpose: Stores customer details enriched with demographic and geographic data.
   **.** Columns:
   **|Column Name | Data Type | Description|**
     customer_key     INT      Surrogate key uniquely identifying each cusotmer record in the dimension table   
     customer_id      INT               Unique mumerical identifier assigned to each customer        
