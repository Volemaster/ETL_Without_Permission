USE NorthwindCopy

/*  Simple example -- we're going to use SELECT... INTO to simulate loading     *
 *  remote data into a temp table so we can focus on the SQL                    */

-- Set up the temp tables:
  DROP TABLE IF EXISTS #Suppliers, #View;

  SELECT * INTO #Suppliers FROM Northwind.dbo.Suppliers 
  SELECT * INTO #View FROM Northwind.dbo.[Alphabetical list of products]

RETURN

SELECT * FROM #Suppliers;
SELECT DISTINCT CategoryId, CategoryName FROM #View
SELECT ProductId, ProductName, SupplierId, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued FROM #View

RETURN

/*  The simple example: loading without having to transform the primary key at all.     *
 *  If you have permissions to do IDENTITY_INSERT on a production table, you probably   *
 *  either (1) need a new DBA, or (2) just need to ask your DBA for access to           *
 *  bulk loading functionality; if the DBA is giving away ALTER TABLE permissions,      *
 *  you can probably get BULK OPERATIONS, too, but you may as well just ask for         *
 *  db_owner to make it simpler for everyone.                                           *
 *                                                                                      *
 *  This is actually meant to simulate a case where you're inserting into a table       *
 *  where the PK isn't an identity field, so just ignore the IDENTITY_INSERT part       *
 *  for now.                                                                            */

USE NorthwindCopy
BEGIN TRANSACTION

/*  Pretend this isn't here...  */ SET IDENTITY_INSERT dbo.Suppliers ON;

;WITH Source AS (
  -- We don't want to add exact duplicates...
  SELECT CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax, CAST(HomePage AS NVarchar(Max)) AS HomePage FROM #Suppliers
    -- And we aren't allowed to have PK duplicates even if the core record is different.
    -- If you actually ran into this scenario you'd need to think about what your exception
    -- handling process should look like, which would probably be business specific.
    WHERE SupplierId NOT IN (SELECT SupplierId FROM dbo.Suppliers)
  EXCEPT 
  SELECT CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax, CAST(HomePage AS NVarchar(Max)) FROM dbo.Suppliers
	)
INSERT INTO dbo.Suppliers (SupplierId, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax, HomePage)
SELECT SupplierId, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax, HomePage FROM #Suppliers;

/*  Pretend this isn't here...  */ SET IDENTITY_INSERT dbo.Suppliers OFF; SET IDENTITY_INSERT dbo.Categories ON;

;WITH Source AS (
-- This doesn't contemplate the possibility that a CategoryId could exist
-- but have a different CategoryName. Again, this may be business specific.
-- Here we're going to allow it to crash out if that happens.
SELECT DISTINCT CategoryId, CategoryName FROM #View
EXCEPT
SELECT CategoryId, CategoryName FROM dbo.Categories
)
INSERT INTO dbo.Categories(CategoryId, CategoryName)
SELECT CategoryId, CategoryName FROM Source;

/*  Pretend this isn't here...  */ SET IDENTITY_INSERT dbo.Categories OFF; SET IDENTITY_INSERT dbo.Products ON;

;WITH Source AS (
-- This doesn't contemplate the possibility that a ProductId could exist
-- but have a different underlying product. Again, this may be business specific.
-- Here we're going to allow it to crash out if that happens.
SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued FROM #View
EXCEPT
SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued FROM dbo.Products
)
INSERT INTO dbo.Products (ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued)
SELECT ProductId, ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued FROM Source;

/*  Pretend this isn't here...  */ SET IDENTITY_INSERT dbo.Products OFF;

SELECT * FROM dbo.Categories;
SELECT * FROM dbo.Products;
ROLLBACK
