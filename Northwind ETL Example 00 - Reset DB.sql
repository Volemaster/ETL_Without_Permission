use NorthwindCopy

/*  Reset the fields for the demo */

DELETE FROM dbo.Products
DELETE FROM dbo.Suppliers
DELETE FROM dbo.Categories
DBCC CHECKIDENT ("dbo.Suppliers", RESEED, 0);
DBCC CHECKIDENT ("dbo.Categories", RESEED, 0);
DBCC CHECKIDENT ("dbo.Products", RESEED, 0);

SELECT * FROM dbo.Products
SELECT * FROM dbo.Categories
SELECT * FROM dbo.Suppliers
