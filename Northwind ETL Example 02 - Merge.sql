/*  Better example. Here we're going to actually address the problem of         *
 *  having data partially loaded, and of resolving key mismatches between       *
 *  the source and target data.                                                 */

SET NOCOUNT ON
DECLARE @Description NVarchar(4000);

/*  The "--! Begin Section:" and "--! End Section:" tags are parsed out     *
 *  when we execute this script using the tool so that we can use the       *
 *  tool's ETL functionality. Anything between the tags is ignored outside  *
 *  of SSMS.                                                                */

--! Begin Section: Manual
  USE NorthwindCopy
  DROP TABLE IF EXISTS #Suppliers, #View;
  /*  Set up the temp tables: */
  SELECT * INTO #Suppliers FROM Northwind.dbo.Suppliers 
  SELECT * INTO #View FROM Northwind.dbo.[Alphabetical list of products]
--! End Section: Manual

  DROP TABLE IF EXISTS #SupplierMap, #CategoryMap, #ProductMap;

/*  Now that we've added the data, let's add some indexes.                  *
 *  Normally you would create the PKs when you create the temp tables       *
 *  but since we did SELECT...INTO, we can't do that here, so we will       *
 *  have to create the PKs after the fact using dynamic SQL to prevent      *
 *  name collisions. (Indexes can reuse names as long as they have          *
 *  unique names across the object that they index, but key constraints     *
 *  have to be globally unique.)                                            *
 *                                                                          *
 *  PKs first, so that the indexes don't need to be rebuilt to reference    *
 *  the PK instead of the ROWID                                             */

  DECLARE @SQL NVarchar(Max);
  IF EXISTS (SELECT 1 FROM tempdb.sys.columns where object_id=object_id('tempdb..#Suppliers') and name = 'SupplierId' and is_nullable=1)
    ALTER TABLE #Suppliers ALTER COLUMN SupplierId INT NOT NULL

  SET @SQL = 'ALTER TABLE #Suppliers ADD CONSTRAINT ' + Quotename(Concat('Suppliers_', NewId())) + ' PRIMARY KEY (SupplierId);'
  PRINT @SQL; EXEC sp_executesql @SQL;

  IF EXISTS (SELECT 1 FROM tempdb.sys.columns where object_id=object_id('tempdb..#View') and name = 'ProductId' and is_nullable=1)
    ALTER TABLE #View ALTER COLUMN ProductId INT NOT NULL

  SET @SQL = 'ALTER TABLE #View ADD CONSTRAINT ' + Quotename(Concat('View_', NewId())) + ' PRIMARY KEY (ProductId);'
  PRINT @SQL; EXEC sp_executesql @SQL;

  CREATE UNIQUE INDEX SuppliersCompanyName ON #Suppliers (CompanyName);
  CREATE INDEX ViewCategoryName ON #View (CategoryName);
  CREATE INDEX ViewCategoryCompound ON #View (CategoryId, CategoryName);
  CREATE INDEX ViewCategorySupplierId ON #View (SupplierId);

/*  The _____Map tables are to handle changes in primary keys that impact the foreign
    keys of other data we're loading. */

  CREATE TABLE #SupplierMap (OldId INT NOT NULL PRIMARY KEY, SupplierId INT NOT NULL);
  CREATE UNIQUE INDEX SupplierMapUnique ON #SupplierMap (SupplierId);

  CREATE TABLE #CategoryMap (OldId INT NOT NULL PRIMARY KEY, CategoryId INT NOT NULL);
  CREATE UNIQUE INDEX CategoryMapUnique ON #CategoryMap (CategoryId);

/*  ProductMap isn't needed because we're not loading anything that references
    ProductId as part of this exercise. */

    --CREATE TABLE #ProductMap (OldId INT NOT NULL PRIMARY KEY, ProductId INT NOT NULL);
    --CREATE UNIQUE INDEX ProductMapUnique ON #ProductMap (ProductId);

SET NOCOUNT OFF

BEGIN TRANSACTION

/*  SUPPLIER SECTION  */
BEGIN
SET @Description = 'SUPPLIER SECTION'; PRINT @Description;
SET @Description = '  Delete duplicate suppliers:' ; PRINT @Description;

/*  Delete all true duplicates */
DELETE FROM #Suppliers 
WHERE SupplierId IN 
  ( SELECT SupplierId FROM 
  /*  Intersect is the easiest way to deal with NULL comparisons  */
    ( SELECT SupplierId, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax, CAST(HomePage AS NVarchar(Max)) AS HomePage FROM #Suppliers
      INTERSECT 
      SELECT SupplierId, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax, CAST(HomePage AS NVarchar(Max)) FROM dbo.Suppliers
    ) AS X
  )


/*  If a record already exists that matches the supplier exactly,   *
 *  except for the SupplierId, remap the imported supplier ID       *
 *  to point to the actual supplier ID already in the table         */

SET @Description = '  Map exactly matched supplier PKs:' ; PRINT @Description;
INSERT INTO #SupplierMap (OldId, SupplierId)
SELECT #Suppliers.SupplierID AS OldId, Suppliers.SupplierID FROM #Suppliers, dbo.Suppliers
WHERE EXISTS (
      SELECT #Suppliers.CompanyName, #Suppliers.ContactName, #Suppliers.ContactTitle, #Suppliers.Address, #Suppliers.City, #Suppliers.Region, #Suppliers.PostalCode, #Suppliers.Country, #Suppliers.Phone, #Suppliers.Fax, CAST(#Suppliers.HomePage AS NVarchar(Max)) 
      INTERSECT 
      SELECT Suppliers.CompanyName, Suppliers.ContactName, Suppliers.ContactTitle, Suppliers.Address, Suppliers.City, Suppliers.Region, Suppliers.PostalCode, Suppliers.Country, Suppliers.Phone, Suppliers.Fax, CAST(Suppliers.HomePage AS NVarchar(Max)) 
)

/*  Now for the fun!                                                  *
 *  We're using a MERGE that forces an INSERT in order to             *
 *  be able to get the original SupplierId and the NEW SupplierId     *
 *  so that we can map products from SupplierId in the other database *
 *  to the SupplierId in this one.                                    */

SET @Description = '  Load suppliers to dbo.Suppliers:' ; PRINT @Description;
;WITH Source AS (
  SELECT * FROM #Suppliers WHERE SupplierId NOT IN (SELECT OldId FROM #SupplierMap)
)
MERGE INTO dbo.Suppliers AS Target 
USING Source ON 1=0
WHEN NOT MATCHED BY TARGET THEN
INSERT (CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax, HomePage)
VALUES (CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax, HomePage)
OUTPUT Source.SupplierId, Inserted.SupplierId
INTO #SupplierMap (OldId, SupplierId);

/*  Mapped values, if you are interested... */
--SELECT #SupplierMap.SupplierId AS NewSupplierId, #Suppliers.* FROM #Suppliers INNER JOIN #SupplierMap ON #Suppliers.SupplierID=#SupplierMap.OldId;
END /*  END OF SUPPLIER SECTION  */


/*  CATEGORY SECTION  */
BEGIN
/*  Since the category and product data is shared in a single temp table,   *
 *  we can't just delete the rows where the category already exists; we     *
 *  have to go remove them from the query itself (unfortunately).           */


/*  If you need to minimize the duration exclusive lock on the Categories   *
 *  table front-loading some of the data comparison is one way to do that.  *
 *                                                                          *
 *  An example of another case where doing this might make sense would be   *
 *  if the CategoryName field wasn't indexed on a large table (indexed or   *
 *  not, with only 8 nvarchar(15) records it really doesn't matter).        *
 *                                                                          *
 *  This isn't really reasonable in this particular case, but we're going   *
 *  to demonstrate it anyway...                                             */
SET @Description = '  CATEGORY SECTION' ; PRINT @Description;

DROP TABLE IF EXISTS #CategoryParanoidWay;
CREATE TABLE #CategoryParanoidWay (CategoryId INT NOT NULL PRIMARY KEY, CategoryName NVarchar(15) NOT NULL);
CREATE INDEX CategoryParanoidWay_CategoryName ON #CategoryParanoidWay (CategoryName);

SET @Description = '  Populate #CategoryParanoidWay' ; PRINT @Description;
INSERT INTO #CategoryParanoidWay (CategoryId, CategoryName) 
SELECT CategoryId, CategoryName FROM dbo.Categories;

/*  Based on the way the tables are structured, we can't infer that         *
 *  CategoryName is supposed to be unique... but it really doesn't make     *
 *  sense to have duplicate values for it, so we're going to consult with   *
 *  the business and confirm that it's supposed to be unique (whatever      *
 *  the database schema might claim).                                       *
 *                                                                          *
 *  (In all seriousness, though, keep in mind that the actual business      *
 *  logic CAN be more restrictive than the database logic, and use some     *
 *  common sense when making decisions about how to map/load data.)         *
 *                                                                          *
 *  Since this is my exercise, naturally they agreed that CategoryName      *
 *  should be unique.                                                       *
 *                                                                          *
 *  Based on that, we're going to map the CategoryId keys by matching       *
 *  CategoryName values as well.                                            *
 *                                                                          */

SET @Description = '  Map exactly matched category PKs:' ; PRINT @Description;
INSERT INTO #CategoryMap (OldId, CategoryId)
SELECT DISTINCT #View.CategoryID, Categories.CategoryID 
FROM #View 
INNER JOIN #CategoryParanoidWay AS Categories
ON
/*  Same category, different ID. If the IDs match no mapping is required.   */
  #View.CategoryName=Categories.CategoryName 
  AND #View.CategoryID<>Categories.CategoryID;

SET @Description = '  Load to dbo.Categories:' ; PRINT @Description;
WITH Source AS (
  SELECT DISTINCT CategoryID, CategoryName FROM #View
  WHERE CategoryName NOT IN (SELECT CategoryName FROM #CategoryParanoidWay)
)
MERGE INTO dbo.Categories AS Target 
USING Source ON 1=2
WHEN NOT MATCHED BY TARGET THEN
INSERT (CategoryName) VALUES (CategoryName)
OUTPUT Source.CategoryId, Inserted.CategoryId
INTO #CategoryMap (OldId, CategoryId);

/*  Mapped values, if you are interested... */
--SELECT #CategoryMap.CategoryId AS NewCategoryId, #View.CategoryID, #View.CategoryName FROM #View INNER JOIN #CategoryMap ON #View.CategoryID=#CategoryMap.OldId;
END /*  END OF CATEGORY SECTION  */


/*  Finally... Products.                                                            *
 *  Since we're not going to use ProductId for anything, we can do a simple INSERT, *
 *  but remember that we need to include any key mappings where they are present,   *
 *  but the FK value itself if it isn't mapped to a different FK value on this DB.  */

SET @Description = 'PRODUCT SECTION' ; PRINT @Description;
SET @Description = '  Load to dbo.Products' ; PRINT @Description;
;WITH Source AS (
SELECT 
  ProductName, COALESCE(SM.SupplierId, V.SupplierId) AS SupplierId, COALESCE(CM.CategoryId, V.CategoryId) AS CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued 
FROM 
  #View AS V
LEFT JOIN #SupplierMap AS SM 
  ON 
    V.SupplierID=SM.OldId 
LEFT JOIN #CategoryMap AS CM 
  ON 
    V.CategoryID=CM.OldId
EXCEPT
SELECT 
  ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued 
FROM 
  dbo.Products
)
INSERT INTO dbo.Products 
  (ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued)
SELECT 
  ProductName, SupplierId, CategoryId, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued 
FROM 
  Source;

COMMIT
--! Begin Section: Manual
SET NOCOUNT ON
SELECT * FROM dbo.Products
SELECT * FROM dbo.Categories
SELECT * FROM dbo.Suppliers
SET NOCOUNT OFF
RETURN

/*  Simple testing of what happens when records are partially present */
DROP TABLE IF EXISTS #CDelete, #SDelete;
CREATE TABLE #CDelete(CategoryId INT NOT NULL PRIMARY KEY);
CREATE TABLE #SDelete(SupplierId INT NOT NULL PRIMARY KEY);

INSERT INTO #CDelete (CategoryId) SELECT CategoryId FROM (SELECT CategoryId, ROW_NUMBER() OVER(ORDER BY CategoryId) AS RowNumber FROM dbo.Categories) AS X WHERE RowNumber % 2 = 0
INSERT INTO #SDelete (SupplierId) SELECT SupplierId FROM (SELECT SupplierId, ROW_NUMBER() OVER(ORDER BY SupplierId) AS RowNumber FROM dbo.Suppliers) AS X WHERE RowNumber % 3 = 0

DELETE FROM dbo.Products WHERE CategoryId IN (SELECT CategoryId FROM #CDelete) OR SupplierId IN (SELECT SupplierID FROM #SDelete)
DELETE FROM dbo.Categories WHERE categoryId IN (SELECT CategoryId FROM #CDelete)
DELETE FROM dbo.Suppliers WHERE SupplierId IN (SELECT SupplierID FROM #SDelete)

--! End Section: Manual