SELECT 
            item_id,
            brand,
            item_name,
            quantity,
            item_description,
            parent_category,
            sales_category,
            price,
            currency,
            competitor_name,
            catalog_name,
            catalog_start,
            catalog_end,
            page
        FROM competitor_item_details
        -- WHERE sales_category IN ('Alcohol', 'DF Spirits')