DROP DATABASE IF EXISTS TBWalmart;
CREATE DATABASE TBWalmart;
USE TBWalmart;
CREATE TABLE Customer_Dim (
    Customer_ID VARCHAR(20) PRIMARY KEY,
    Gender CHAR(1),
    Age_Range VARCHAR(10),
    Occupation INT,
    City_Category CHAR(1),
    Stay_In_Current_City_Years VARCHAR(5),
    Marital_Status INT
);
CREATE TABLE Store_Dim (
    Store_ID INT PRIMARY KEY,
    Store_Name VARCHAR(100)
);
CREATE TABLE Supplier_Dim (
    Supplier_ID INT PRIMARY KEY,
    Supplier_Name VARCHAR(100)
);
CREATE TABLE Product_Dim (
    Product_ID VARCHAR(20) PRIMARY KEY,
    Product_Category VARCHAR(50),
    Price DECIMAL(10,2),
    Store_ID INT,
    Supplier_ID INT,
    FOREIGN KEY (Store_ID) REFERENCES Store_Dim(Store_ID),
    FOREIGN KEY (Supplier_ID) REFERENCES Supplier_Dim(Supplier_ID)
);
CREATE TABLE Date_Dim (
    Date_ID INT PRIMARY KEY,               
    Full_Date DATE,
    Day INT,
    Month INT,
    Month_Name VARCHAR(15),
    Quarter VARCHAR(5),
    Year INT,
    Weekday_Weekend VARCHAR(10),
    Season VARCHAR(10)
);
CREATE TABLE Sales_Fact (
    Sales_ID INT AUTO_INCREMENT PRIMARY KEY,
    OrderID INT,
    Customer_ID VARCHAR(20),
    Product_ID VARCHAR(20),
    Store_ID INT,
    Supplier_ID INT,
    Date_ID INT,
    Quantity INT,
    Total_Amount DECIMAL(12,2),
    Revenue DECIMAL(12,2),

    FOREIGN KEY (Customer_ID) REFERENCES Customer_Dim(Customer_ID),
    FOREIGN KEY (Product_ID) REFERENCES Product_Dim(Product_ID),
    FOREIGN KEY (Store_ID) REFERENCES Store_Dim(Store_ID),
    FOREIGN KEY (Supplier_ID) REFERENCES Supplier_Dim(Supplier_ID),
    FOREIGN KEY (Date_ID) REFERENCES Date_Dim(Date_ID)
);