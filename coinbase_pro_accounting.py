# Implement hifo and fifo accounting for coinbase pro csvs
from csv import DictReader, DictWriter
from datetime import datetime
from enum import Enum
from copy import deepcopy

class Accounting(Enum):
    HIFO = 0
    FIFO = 1
    # TODO LIFO = 2

def load_csv(input_csv_file):
    ''' return a list of all rows in a csv, 
        keyed and sorted by datetime '''
    output = list()
    products = set() # "ETH-USD"
    with open(input_csv_file,  newline='') as f:
        reader = DictReader(f)
        for row in reader:
            products.add(row["product"])
            time = datetime.fromisoformat(row["created at"].rstrip('Z'))
            row['size'] = float(row['size'])
            row['price'] = float(row['price'])
            output.append((time, row))
    assert len(products) == 1, f"Found more than one product: {products}"
    print(f"Processing product: {list(products)[0]}")
    return reader.fieldnames, output

class PairedSale:
    def __init__(self, sale_date, sale_order, buy_date, buy_order, accounting_method):
        self.sale_date = sale_date
        self.sale_order = deepcopy(sale_order)
        self.buy_date = buy_date
        self.buy_order = deepcopy(buy_order)
        self.accounting_method = accounting_method

        assert sale_order['size'] == buy_order['size']

        # TODO add fees after pro-rating 'fee', and 'total' when reducing order sizes
        self.net_proceeds = sale_order['size'] *  sale_order['price']
        self.cost_basis = buy_order['size'] *  buy_order['price']

        gain_loss = self.net_proceeds - self.cost_basis
        long_term = (self.sale_date - self.buy_date).days > 365
        if long_term:
            self.short_term_gains = 0
            self.long_term_gains = gain_loss
        else:
            self.long_term_gains = 0
            self.short_term_gains = gain_loss

def pair_sales_with_buys(orders, year_to_accounting_method, max_year):
    # sort orders chronologically
    orders = sorted(orders, key=lambda x: (x[0], x[1]['trade id']))

    # filter out data more recent than year
    orders_to_ignore = [] 
    orders_to_process = []
    original_total_buy_size = 0
    for date_time, o in orders:
        if date_time.year > max_year:
            orders_to_ignore.append((date_time, o))
        else:
            if o['side'] == "BUY":
                original_total_buy_size += o['size']
            orders_to_process.append((date_time, o))

    # pair sales with buys and remove paired sales/buys from orders
    current_buys = []
    paired_sales = []
    for date_time, o in orders_to_process:
        assert o['side'] in ("BUY", "SELL")
        if o['side'] == "BUY":
            current_buys.append((date_time, o))
        else:
            accounting_method = year_to_accounting_method[date_time.year]
            if  accounting_method == Accounting.HIFO:
                current_buys.sort(key=lambda x: x[1]['price'], reverse=True)
            else:
                current_buys.sort(key=lambda x: (x[0], x[1]['trade id']))
            while o['size']:
                assert current_buys, "current_buys can't cover total_sold"
                covering_datetime, covering_buy = current_buys[0]
                covering_size = covering_buy['size']
                if covering_size <= o['size']:
                    o_copy = deepcopy(o)
                    o_copy['size'] = covering_size
                    paired_sales.append(
                        PairedSale(date_time, o_copy, covering_datetime, covering_buy, accounting_method))
                    current_buys.pop(0)
                    o['size'] -= covering_size
                else:
                    buy_copy = deepcopy(covering_buy)
                    buy_copy['size'] = o['size']
                    paired_sales.append(
                        PairedSale(date_time, o, covering_datetime, buy_copy, accounting_method))
                    covering_buy['size'] -= o['size']
                    o['size'] = 0

    # re-sort chronologically like FIFO
    current_buys.sort(key=lambda x: (x[0], x[1]['trade id']))

    # Do some data validation
    total_modified_buy_size = sum([x[1]['size'] for x in current_buys])
    total_sale_size = sum([x.sale_order['size'] for x in paired_sales])
    assert original_total_buy_size - total_modified_buy_size - total_sale_size < 0.000001

    modified_orders = current_buys + orders_to_ignore
    return (modified_orders, paired_sales)

def main(input_csv, year_to_accounting_method, max_year):
    input_fieldnames, orders = load_csv(input_csv)
    modified_orders, paired_sales = pair_sales_with_buys(orders, year_to_accounting_method, max_year)

    # Write modified orders and sales to csvs
    output_csv_base, _ = input_csv.rsplit(".", 1)

    modified_orders_csv = f"{max_year}.{output_csv_base}.unpaired_orders.csv"
    with open(modified_orders_csv, 'w', newline='') as out_csv:
        writer = DictWriter(out_csv, fieldnames=input_fieldnames)
        writer.writeheader()
        for _, o in modified_orders:
            writer.writerow(o)

    paired_sales_csv = f"{max_year}.{output_csv_base}.paired_orders.csv"
    with open(paired_sales_csv, 'w', newline='') as out_csv:
        writer = DictWriter(out_csv, fieldnames=input_fieldnames)
        writer.writeheader()
        for sale in paired_sales:
            writer.writerow(sale.buy_order)
            writer.writerow(sale.sale_order)

    cap_gains_csv = f"{max_year}.{output_csv_base}.cap_gains_per_sale.csv"
    with open(cap_gains_csv, 'w', newline='') as out_csv:
        writer = DictWriter(out_csv, fieldnames=[
            "buy_id", "sale_id", "buy_date", "sale_date", "size", "buy_price", "sale_price", "net_proceeds", "cost_basis", "short_term_gains", "long_term_gains", "accounting"
        ])
        writer.writeheader()
        for sale in paired_sales:
            output = {
                "buy_id": sale.buy_order['trade id'],
                "sale_id": sale.sale_order['trade id'],
                "buy_date": sale.buy_date,
                "sale_date": sale.sale_date,
                "size": sale.sale_order['size'],
                "buy_price": sale.buy_order['price'],
                "sale_price": sale.sale_order['price'],
                "net_proceeds": sale.net_proceeds,
                "cost_basis": sale.cost_basis,
                "short_term_gains": sale.short_term_gains,
                "long_term_gains": sale.long_term_gains,
                "accounting": sale.accounting_method,
            }
            writer.writerow(output)

if __name__ == "__main__":
    main("coinbase_original_fills_up_to_2021.csv", {2018: Accounting.HIFO, 2021:Accounting.FIFO}, 2021)