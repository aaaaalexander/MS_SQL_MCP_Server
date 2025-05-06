-- Test Database Schema Creation Script
-- This script creates a simple test database structure for SQL MCP Server testing
-- It can be run against a SQL Server instance to set up the necessary tables for testing

-- Create test database
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'test_db')
BEGIN
    CREATE DATABASE test_db;
END
GO

USE test_db;
GO

-- Drop tables if they exist (for clean setup)
IF OBJECT_ID('dbo.OrderItems', 'U') IS NOT NULL DROP TABLE dbo.OrderItems;
IF OBJECT_ID('dbo.Orders', 'U') IS NOT NULL DROP TABLE dbo.Orders;
IF OBJECT_ID('dbo.Products', 'U') IS NOT NULL DROP TABLE dbo.Products;
IF OBJECT_ID('dbo.Customers', 'U') IS NOT NULL DROP TABLE dbo.Customers;
IF OBJECT_ID('dbo.Categories', 'U') IS NOT NULL DROP TABLE dbo.Categories;
GO

-- Create Categories table
CREATE TABLE dbo.Categories (
    CategoryID INT PRIMARY KEY IDENTITY(1,1),
    CategoryName NVARCHAR(50) NOT NULL,
    Description NVARCHAR(200) NULL
);
GO

-- Create Customers table
CREATE TABLE dbo.Customers (
    CustomerID INT PRIMARY KEY IDENTITY(1,1),
    FirstName NVARCHAR(50) NOT NULL,
    LastName NVARCHAR(50) NOT NULL,
    Email NVARCHAR(100) NULL,
    Phone NVARCHAR(20) NULL,
    Address NVARCHAR(100) NULL,
    City NVARCHAR(50) NULL,
    State NVARCHAR(20) NULL,
    ZipCode NVARCHAR(10) NULL,
    CreatedDate DATETIME DEFAULT GETDATE()
);
GO

-- Create Products table
CREATE TABLE dbo.Products (
    ProductID INT PRIMARY KEY IDENTITY(1,1),
    ProductName NVARCHAR(100) NOT NULL,
    CategoryID INT FOREIGN KEY REFERENCES dbo.Categories(CategoryID),
    UnitPrice DECIMAL(10, 2) NOT NULL,
    InStock BIT DEFAULT 1,
    Description NVARCHAR(500) NULL
);
GO

-- Create Orders table
CREATE TABLE dbo.Orders (
    OrderID INT PRIMARY KEY IDENTITY(1,1),
    CustomerID INT FOREIGN KEY REFERENCES dbo.Customers(CustomerID),
    OrderDate DATETIME DEFAULT GETDATE(),
    ShipDate DATETIME NULL,
    Status NVARCHAR(20) DEFAULT 'Pending',
    TotalAmount DECIMAL(10, 2) NULL
);
GO

-- Create OrderItems table
CREATE TABLE dbo.OrderItems (
    OrderItemID INT PRIMARY KEY IDENTITY(1,1),
    OrderID INT FOREIGN KEY REFERENCES dbo.Orders(OrderID),
    ProductID INT FOREIGN KEY REFERENCES dbo.Products(ProductID),
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(10, 2) NOT NULL,
    Subtotal AS (Quantity * UnitPrice)
);
GO

-- Insert sample data
-- Categories
INSERT INTO dbo.Categories (CategoryName, Description)
VALUES 
    ('Electronics', 'Electronic gadgets and devices'),
    ('Clothing', 'Apparel and fashion items'),
    ('Books', 'Books and publications'),
    ('Home', 'Home goods and furniture'),
    ('Sports', 'Sports equipment and gear');
GO

-- Customers
INSERT INTO dbo.Customers (FirstName, LastName, Email, Phone, Address, City, State, ZipCode)
VALUES
    ('John', 'Smith', 'john.smith@example.com', '555-123-4567', '123 Main St', 'Springfield', 'IL', '62701'),
    ('Jane', 'Doe', 'jane.doe@example.com', '555-234-5678', '456 Elm St', 'Shelbyville', 'IL', '62565'),
    ('Robert', 'Johnson', 'robert.j@example.com', '555-345-6789', '789 Oak St', 'Capital City', 'IL', '62701'),
    ('Lisa', 'Brown', 'lisa.brown@example.com', '555-456-7890', '321 Pine St', 'Springfield', 'IL', '62704'),
    ('Michael', 'Wilson', 'michael.wilson@example.com', '555-567-8901', '654 Maple St', 'Shelbyville', 'IL', '62565');
GO

-- Products
INSERT INTO dbo.Products (ProductName, CategoryID, UnitPrice, InStock, Description)
VALUES
    ('Smartphone', 1, 699.99, 1, 'Latest model smartphone with advanced features'),
    ('Laptop', 1, 1299.99, 1, 'Powerful laptop for work and gaming'),
    ('T-Shirt', 2, 19.99, 1, 'Cotton t-shirt, various colors available'),
    ('Jeans', 2, 49.99, 1, 'Classic blue jeans, slim fit'),
    ('Novel: The Adventure', 3, 14.99, 1, 'Bestselling adventure novel'),
    ('Cookbook', 3, 24.99, 0, 'Collection of gourmet recipes'),
    ('Sofa', 4, 599.99, 1, 'Comfortable 3-seater sofa'),
    ('Coffee Table', 4, 199.99, 1, 'Modern design coffee table'),
    ('Basketball', 5, 29.99, 1, 'Official size basketball'),
    ('Tennis Racket', 5, 89.99, 1, 'Professional tennis racket');
GO

-- Orders
INSERT INTO dbo.Orders (CustomerID, OrderDate, ShipDate, Status, TotalAmount)
VALUES
    (1, '2023-01-15', '2023-01-18', 'Delivered', 749.98),
    (2, '2023-01-20', '2023-01-23', 'Delivered', 1299.99),
    (3, '2023-02-05', NULL, 'Processing', 114.97),
    (4, '2023-02-10', '2023-02-15', 'Shipped', 599.99),
    (5, '2023-02-20', NULL, 'Pending', 119.98),
    (1, '2023-03-01', '2023-03-05', 'Delivered', 224.95),
    (2, '2023-03-15', NULL, 'Cancelled', 0.00);
GO

-- OrderItems
INSERT INTO dbo.OrderItems (OrderID, ProductID, Quantity, UnitPrice)
VALUES
    (1, 1, 1, 699.99),
    (1, 3, 2, 19.99),
    (2, 2, 1, 1299.99),
    (3, 5, 1, 14.99),
    (3, 3, 3, 19.99),
    (3, 4, 1, 49.99),
    (4, 7, 1, 599.99),
    (5, 9, 1, 29.99),
    (5, 10, 1, 89.99),
    (6, 8, 1, 199.99),
    (6, 3, 1, 19.99),
    (6, 6, 1, 24.99);
GO

-- Create a view for order summaries
CREATE OR ALTER VIEW dbo.OrderSummaries AS
SELECT 
    o.OrderID,
    o.OrderDate,
    c.CustomerID,
    c.FirstName + ' ' + c.LastName AS CustomerName,
    COUNT(oi.OrderItemID) AS ItemCount,
    SUM(oi.Quantity) AS TotalQuantity,
    SUM(oi.Quantity * oi.UnitPrice) AS OrderTotal,
    o.Status
FROM dbo.Orders o
JOIN dbo.Customers c ON o.CustomerID = c.CustomerID
JOIN dbo.OrderItems oi ON o.OrderID = oi.OrderID
GROUP BY o.OrderID, o.OrderDate, c.CustomerID, c.FirstName, c.LastName, o.Status;
GO

-- Create a stored procedure for getting order details
CREATE OR ALTER PROCEDURE dbo.GetOrderDetails
    @OrderID INT
AS
BEGIN
    SELECT 
        o.OrderID,
        o.OrderDate,
        o.ShipDate,
        o.Status,
        c.CustomerID,
        c.FirstName + ' ' + c.LastName AS CustomerName,
        c.Email,
        p.ProductID,
        p.ProductName,
        cat.CategoryName,
        oi.Quantity,
        oi.UnitPrice,
        oi.Quantity * oi.UnitPrice AS Subtotal
    FROM dbo.Orders o
    JOIN dbo.Customers c ON o.CustomerID = c.CustomerID
    JOIN dbo.OrderItems oi ON o.OrderID = oi.OrderID
    JOIN dbo.Products p ON oi.ProductID = p.ProductID
    JOIN dbo.Categories cat ON p.CategoryID = cat.CategoryID
    WHERE o.OrderID = @OrderID
    ORDER BY p.ProductName;
END;
GO

-- Create a stored procedure for searching products
CREATE OR ALTER PROCEDURE dbo.SearchProducts
    @SearchTerm NVARCHAR(100),
    @CategoryID INT = NULL,
    @MinPrice DECIMAL(10, 2) = NULL,
    @MaxPrice DECIMAL(10, 2) = NULL,
    @InStockOnly BIT = 0
AS
BEGIN
    SELECT 
        p.ProductID,
        p.ProductName,
        c.CategoryName,
        p.UnitPrice,
        p.InStock,
        p.Description
    FROM dbo.Products p
    JOIN dbo.Categories c ON p.CategoryID = c.CategoryID
    WHERE (p.ProductName LIKE '%' + @SearchTerm + '%' OR p.Description LIKE '%' + @SearchTerm + '%')
        AND (@CategoryID IS NULL OR p.CategoryID = @CategoryID)
        AND (@MinPrice IS NULL OR p.UnitPrice >= @MinPrice)
        AND (@MaxPrice IS NULL OR p.UnitPrice <= @MaxPrice)
        AND (@InStockOnly = 0 OR p.InStock = 1)
    ORDER BY p.ProductName;
END;
GO

PRINT 'Test database creation completed successfully.'
GO
