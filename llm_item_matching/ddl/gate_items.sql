SELECT 
            item_row_id,
            item_name,
            item_description,
            item_parent_sales_category_name,
            item_sales_category_name
        FROM item
        -- WHERE item_sales_category_name IN ('Alcohol', 'DF Spirits')