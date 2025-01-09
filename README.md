# NoFrills Data Cleanser

**NoFrills Data Cleanser** is an inventory filtering and cleansing tool designed to ease the inventory management of the store owner that uses it. Each day, upon input of the inventory file of their departments, the application provides the owner with insightful features, such as finding their zero or "low" quantities. 
The problem was borught to me by the owner of Girards No Frills, the store I've worked at for over 3 years now, Joe Girard. Originally he manually reconciled his data using excel functions, copy, and paste. 

Joe knew what to look for in terms of edge cases for the input data, output data, and the significance behind it all. My job, as the programmer was to figure out how to put it all together, but this time **automatically**. 
The product we have arrived at has been in use for over a month now, he expressed to me his gratitude,  telling me  "Thank you, I don't have to do a computer's job anymore". 
Since then, the product has been outsourced to X new owners.


The tool features a simple interface for identifying low and zero inventory quantities between all of a store's departments, and streamlining inventory updates back into the internal system using mouse and keyboard control.

---

## Features

### Zero Quantity Finder

- Identify articles with `inv_qty` of 0 in the inventory file.
- Exclude any zero quantities listed in the `DNO` (Do Not Order) file.

### Low Quantity Finder

- Identify articles with quantities greater than 0 but below a configurable threshold (`low`).
- Default `low` threshold is set to 2 (can be adjusted via the interactive settings).

### Tkinter Interactive Display

- View departments and categories included in the cleanse.
- Filter inventory using interactive buttons.

### Automated Inventory Update

- Using **PyAutoGUI**, update the cleansed quantities in the inventory window by simulating keyboard and mouse inputs with predefined coordinates.

### DNO File Management

- Add or remove articles from the `DNO` file.
- Bulk update `DNO` using a `.txt` file.
- Sync the `DNO` database across multiple devices via email.

### Logging & Monitoring

- Log the amount of data processed.
- Track processed data counts and timestamps.

### Customizable Parameters

- Adjust hyperparameters (e.g., `low` quantity threshold) via a settings window.

---

## Sample Input Files

### Inventory File (`inventory.xlsx`)

```plaintext
dept    category    article    inv_qty
grocery juice       2728       0
meat    pork        3344       0
home    pots        2224       0
bakery  bread       5612       2
```

### Local DNO File (`dno.db`)

```plaintext
article
2728
3344
```

---

## Filtering Methods

### 1. `find_zeros()`

Identifies articles with `inv_qty` of 0 that are **not** in the `DNO` file.

**Example Output**: `2224`

### 2. `find_lows(low_threshold=2)`

Identifies articles with quantities above 0 but below the specified `low_threshold`.

**Example Output**: `5612, 2`

---

## Interactive GUI

### Filter Buttons

- **Find Zeros**: Filter zero-quantity items.
- **Find Lows**: Filter low-quantity items.
- **Send to SAP**: Initiate the automatic input to SAP.

### DNO Management

- **Add Article**: Add an article to the `DNO` file.
- **Remove Article**: Remove an article from the `DNO` file.
- **Bulk Add**: Add multiple articles from a `.txt` file.
- **Sync DNO**: Overwrite the `DNO` database via email sync.

### Owner Settings

- Modify the departments the app tracks.
- Adjust the `low` threshold.
- Set up coordinates for automated input.

---

## Installation

### Dependencies

Install required libraries using pip:

```bash
pip install pandas tkinter pyautogui sqlite3 xlrd
```

### Run the Application

```bash
python main.py
```

---
##Final Notes

Daily, the data cleansing application identifies and rectifies over 100s of inventory discrepencies across all departments, and prepares the data to be input back into the system to receive credit for properly managing your store's inventory. For each store it is employed in, it saves about hour of manual work an owner would otherwise need to do.

The source files displayed in this repository are the product of Chris Ortiz and Joe Girard

## Future Enhancements

- Cloud synchronization for `DNO` files between devices.

---
