CREATE TABLE dbo.competitor_item_details (
    item_id INT PRIMARY KEY,
    competitor_name VARCHAR(200),
    item_name VARCHAR(300),
    Item_description NVARCHAR(4000),
    brand VARCHAR(200),
    quantity VARCHAR(100),
    parent_category VARCHAR(150),
    sales_category VARCHAR(150),
    price DECIMAL(10,2),
    currency VARCHAR(10),
    catalog_name VARCHAR(300),
    catalog_start DATE,
    catalog_end DATE,
    page INT,
    created_at DATETIME DEFAULT GETDATE()
);