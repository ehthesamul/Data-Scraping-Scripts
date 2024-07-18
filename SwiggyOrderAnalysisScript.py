import requests
import json
from collections import Counter
from collections import defaultdict
from datetime import datetime
import argparse
import csv

def save_analysis_to_csv(orders, output_file):
    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        
        writer.writerow(['Order Analysis'])

        # Basic statistics
        writer.writerow(['Total Orders', len(orders)])
        writer.writerow(['Average Order Value', calculate_average_order_value(orders)])
        writer.writerow(['Total Discount', discount_usage(orders)])

        # Frequency analysis
        writer.writerow(['Order Frequency by Date'])
        for date, count in order_frequency(orders).items():
            writer.writerow([date, count])

        writer.writerow(['Monthly Order Frequency'])
        for month, count in monthly_order_frequency(orders).items():
            writer.writerow([month, count])

        # Customer analysis
        writer.writerow(['Customer Analysis'])
        customer_data = customer_analysis(orders)
        writer.writerow(['Customer ID', 'Total Spend', 'Order Count', 'Average Order Value'])
        for customer_id, data in customer_data.items():
            writer.writerow([customer_id, data['total_spend'], data['order_count'], data['average_order_value']])

        # Item analysis
        writer.writerow(['Popular Items'])
        for item, count in popular_items(orders):
            writer.writerow([item, count])

        writer.writerow(['Highest Price Item', *analyze_item_prices(orders)[:3]])
        writer.writerow(['Lowest Price Item', *analyze_item_prices(orders)[3:]])

        # Packing charges analysis
        writer.writerow(['Packing Charges Analysis'])
        packing_charges_data = packing_charges_analysis(orders)
        writer.writerow(['Total Packing Charges', 'Highest Packing Charge', 'Lowest Packing Charge'])
        writer.writerow([packing_charges_data['total_packing_charges'], packing_charges_data['highest_packing_charge'], packing_charges_data['lowest_packing_charge']])

        # Delivery time analysis
        writer.writerow(['Delivery Time Analysis'])
        delivery_time_data = delivery_time_analysis(orders)
        writer.writerow(['Average Delivery Time (hours)', 'Highest Delivery Time (hours)', 'Lowest Delivery Time (hours)'])
        writer.writerow([delivery_time_data['average_delivery_time_hours'], delivery_time_data['highest_delivery_time_hours'], delivery_time_data['lowest_delivery_time_hours']])

        # Geographical analysis
        writer.writerow(['Geographical Analysis'])
        writer.writerow(['Delivery Area', 'Order Count'])
        for area, count in geographical_analysis(orders).items():
            writer.writerow([area, count])

        # Other analysis
        writer.writerow(['Peak Ordering Times'])
        for hour, count in peak_ordering_times(orders).items():
            writer.writerow([hour, count])

        writer.writerow(['Vegetarian vs Non-Vegetarian Items'])
        veg_nonveg_data = veg_nonveg_analysis(orders)
        writer.writerow(['Vegetarian Count', 'Non-Vegetarian Count'])
        writer.writerow([veg_nonveg_data['vegetarian_count'], veg_nonveg_data['non_vegetarian_count']])

        writer.writerow(['Payment Method Usage'])
        payment_counts = count_payment_methods(orders)
        writer.writerow(['Payment Method', 'Count'])
        for method, count in payment_counts.items():
            writer.writerow([method, count])

        writer.writerow(['Order Date Range'])
        oldest_date, latest_date = find_order_dates(orders)
        writer.writerow(['Oldest Date', oldest_date])
        writer.writerow(['Latest Date', latest_date])

        writer.writerow(['Restaurant Distance Analysis'])
        farthest_restaurant, farthest_distance, farthest_count, nearest_restaurant, nearest_distance, nearest_count, total_distance = analyze_restaurant_distance(orders)
        writer.writerow(['Farthest Restaurant', 'Distance (km)', 'Count', 'Nearest Restaurant', 'Distance (km)', 'Count', 'Total Distance (km)'])
        writer.writerow([farthest_restaurant, farthest_distance, farthest_count, nearest_restaurant, nearest_distance, nearest_count, total_distance])

        writer.writerow(['Rain Mode Analysis'])
        rain_count, no_rain_count = analyze_rain_mode(orders)
        writer.writerow(['Orders in Rain', 'Orders without Rain'])
        writer.writerow([rain_count, no_rain_count])

        writer.writerow(['Distance Flag Analysis'])
        long_distance_count, super_long_distance_count = analyze_distance_flags(orders)
        writer.writerow(['Long Distance Orders', 'Super Long Distance Orders'])
        writer.writerow([long_distance_count, super_long_distance_count])

        delivery_boys = analyze_delivery_boys(orders)
        writer.writerow(['Delivery Boy', 'Number of Orders'])
        for delivery_boy, count in delivery_boys.items():
            writer.writerow([delivery_boy, count])

def fetch_all_orders(session_tid):
    base_url = "https://www.swiggy.com/dapi/order/all?order_id="
    headers = {
    "Sec-Ch-Ua": '"Chromium";v="121", "Not A(Brand";v="99"',
    "Content-Type": "application/json",
    "__fetch_req__": "true",
    "Sec-Ch-Ua-Mobile": "?0",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.160 Safari/537.36",
    "Sec-Ch-Ua-Platform": '"Linux"',
    "Accept": "*/*",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "Referer": "https://www.swiggy.com/my-account",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Priority": "u=1, i"
    }

    cookies = {
    "__SW": "mKyga_dsEDsWc2qyLUZ95s5qf51L2xLF",
    "_device_id": "3b6f214e-344f-ed73-3ce5-c26f1e586cfc",
    "_sid": "yj92e8d-e12a-40a4-a4af-324251ae3a77",
    "userLocation": '{"lat":"12.96340","lng":"77.58550","address":"","area":"","showUserDefaultAddressHint":false}',
    "fontsLoaded": "1",
    "_gcl_au": "1.1.1793514671.1720966826",
    "_gid": "GA1.2.1721627970.1720952754",
    "_gat_0": "1",
    "_ga_34JYJ0BCRN": "GS1.1.1720952753.1.0.1720952763.0.0.0",
    "_is_logged_in": "1",
    "_session_tid": session_tid,
    "_ga": "GA1.2.2091806800.1720952754",
    "_gat_UA-53591212-4": "1"
    }   
    next_order_id = ""  # Initialize the first request without order_id parameter
    all_orders = []  # List to store all orders

    while True:
        url = base_url + next_order_id if next_order_id else base_url
        response = requests.get(url, cookies=cookies, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            if 'data' in data and 'orders' in data['data']:
                orders = data['data']['orders']
                all_orders.extend(orders)
                
                if len(orders) > 0:
                    next_order_id = str(orders[-1]['order_id'])  # Use the last order_id for the next request
                else:
                    break  # No more orders to fetch
            else:
                print("No orders found in response data.")
                break
        else:
            print(f"Failed to fetch orders. Status code: {response.status_code}")
            break
    if all_orders:
        with open('all_orders.json', 'w') as f:
            json.dump(all_orders, f, indent=4)
        print("All orders saved to all_orders.json file.")
    else:
        print("No orders fetched or saved.")
    return all_orders

def calculate_total_order_amount(order):
    total = 0
    for item in order.get('order_items', []):
        total += float(item.get('final_price', 0))
    for charge in order.get('charges', {}).values():
        total += float(charge)
    return total

def calculate_average_order_value(orders):
    total = sum(calculate_total_order_amount(order) for order in orders)
    return total / len(orders) if orders else 0

def order_frequency(orders):
    dates = [order['order_time'][:10] for order in orders]  # Extract date part
    return Counter(dates)

def monthly_order_frequency(orders):
    months = [datetime.strptime(order['order_time'], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m') for order in orders]
    return Counter(months)

def popular_items(orders):
    item_counter = Counter()
    for order in orders:
        for item in order.get('order_items', []):
            item_counter[item['name']] += 1
    return item_counter.most_common()

def order_status_distribution(orders):
    return Counter(order['order_status'] for order in orders)

def customer_analysis(orders):
    customer_data = {}
    for order in orders:
        customer_id = order['customer_id']
        if customer_id not in customer_data:
            customer_data[customer_id] = {
                'total_spend': 0,
                'order_count': 0
            }
        customer_data[customer_id]['total_spend'] += calculate_total_order_amount(order)
        customer_data[customer_id]['order_count'] += 1
    for customer in customer_data.values():
        customer['average_order_value'] = customer['total_spend'] / customer['order_count']
    return customer_data

def discount_usage(orders):
    total_discount = 0
    for order in orders:
        for item in order.get('order_items', []):
            total_discount += float(item.get('item_total_discount', 0))
    return total_discount

def geographical_analysis(orders):
    area_counter = Counter()
    for order in orders:
        area_counter[order['delivery_address']['address']] += 1
    return area_counter

def geographical_analysis_resturant(orders):
    area_counter = Counter()
    for order in orders:
        area_counter[order['restaurant_name']] += 1
    return area_counter

def peak_ordering_times(orders):
    ordering_times = defaultdict(int)

    for order in orders:
        order_hour = datetime.strptime(order['order_time'], '%Y-%m-%d %H:%M:%S').strftime('%H:00')
        ordering_times[order_hour] += 1

    return ordering_times

def veg_nonveg_analysis(orders):
    veg_count = 0
    non_veg_count = 0
    for order in orders:
        for item in order.get('order_items', []):
            if item.get('is_veg') == "1":
                veg_count += 1
            else:
                non_veg_count += 1
    return {'vegetarian_count': veg_count, 'non_vegetarian_count': non_veg_count}

def packing_charges_analysis(orders):
    total_packing_charges = 0
    valid_packing_charges = []
    for order in orders:
        for item in order.get('order_items', []):
            packing_charge = float(item.get('packing_charges', 0))
            if packing_charge > 0:
                valid_packing_charges.append(packing_charge)
                total_packing_charges += packing_charge
    if valid_packing_charges:
        highest_packing_charge = max(valid_packing_charges)
        lowest_packing_charge = min(filter(lambda x: x > 0, valid_packing_charges))
    else:
        highest_packing_charge = 0
        lowest_packing_charge = 0
    return {
        'total_packing_charges': total_packing_charges,
        'highest_packing_charge': highest_packing_charge,
        'lowest_packing_charge': lowest_packing_charge
    }

def delivery_time_analysis(orders):
    total_delivery_time_seconds = 0
    valid_delivery_times = []
    for order in orders:
        delivery_time_seconds = int(order.get('delivery_time_in_seconds', 0))
        if delivery_time_seconds > 0:
            valid_delivery_times.append(delivery_time_seconds)
            total_delivery_time_seconds += delivery_time_seconds
    if valid_delivery_times:
        highest_delivery_time_hours = max(valid_delivery_times) / 60
        lowest_delivery_time_hours = min(filter(lambda x: x > 0, valid_delivery_times)) / 60
    else:
        highest_delivery_time_hours = 0
        lowest_delivery_time_hours = 0
    average_delivery_time_hours = total_delivery_time_seconds / len(valid_delivery_times) / 60 if valid_delivery_times else 0
    return {
        'average_delivery_time_hours': average_delivery_time_hours,
        'highest_delivery_time_hours': highest_delivery_time_hours,
        'lowest_delivery_time_hours': lowest_delivery_time_hours
    }

def count_payment_methods(orders):
    payment_counts = {}
    for order in orders:
        payment_method = order.get('payment_method')
        if payment_method:
            if payment_method in payment_counts:
                payment_counts[payment_method] += 1
            else:
                payment_counts[payment_method] = 1
    return payment_counts

def find_order_dates(orders):
    dates = [datetime.strptime(order['order_time'], "%Y-%m-%d %H:%M:%S") for order in orders]
    oldest_date = min(dates).strftime("%Y-%m-%d %H:%M:%S")
    latest_date = max(dates).strftime("%Y-%m-%d %H:%M:%S")
    return oldest_date, latest_date

def analyze_item_prices(orders):
    item_prices = {}
    for order in orders:
        for item in order['order_items']:
            name = item['name']
            price = float(item['final_price'])
            if name in item_prices:
                item_prices[name].append(price)
            else:
                item_prices[name] = [price]
    
    highest_price_item = max(item_prices.items(), key=lambda x: max(x[1]))
    lowest_price_item = min(item_prices.items(), key=lambda x: min(x[1]) if min(x[1]) > 0 else float('inf'))

    highest_price = max(highest_price_item[1])
    highest_price_count = highest_price_item[1].count(highest_price)
    highest_price_name = highest_price_item[0]

    lowest_price = min(lowest_price_item[1])
    lowest_price_count = lowest_price_item[1].count(lowest_price)
    lowest_price_name = lowest_price_item[0]

    return (highest_price_name, highest_price, highest_price_count, lowest_price_name, lowest_price, lowest_price_count)

def analyze_restaurant_distance(orders):
    distances = {}
    total_distance = 0.0
    for order in orders:
        name = order['restaurant_name']
        distance = float(order['restaurant_customer_distance'])
        total_distance += distance
        if name in distances:
            distances[name].append(distance)
        else:
            distances[name] = [distance]

    # Filter out zero distances and remove entries with no valid distances
    distances = {name: [d for d in dists if d > 0] for name, dists in distances.items()}
    distances = {name: dists for name, dists in distances.items() if dists}

    if not distances:
        raise ValueError("No valid restaurant distances found.")

    farthest_restaurant = max(distances.items(), key=lambda x: max(x[1]))
    nearest_restaurant = min(distances.items(), key=lambda x: min(x[1]))

    farthest_distance = max(farthest_restaurant[1])
    farthest_count = farthest_restaurant[1].count(farthest_distance)
    farthest_name = farthest_restaurant[0]

    nearest_distance = min(nearest_restaurant[1])
    nearest_count = nearest_restaurant[1].count(nearest_distance)
    nearest_name = nearest_restaurant[0]

    return (farthest_name, farthest_distance, farthest_count, nearest_name, nearest_distance, nearest_count, total_distance)

def analyze_rain_mode(orders):
    rain_count = 0
    no_rain_count = 0
    for order in orders:
        if order['rain_mode'] == '1':
            rain_count += 1
        else:
            no_rain_count += 1
    return rain_count, no_rain_count

def analyze_distance_flags(orders):
    long_distance_count = 0
    super_long_distance_count = 0
    for order in orders:
        if order['is_long_distance']:
            long_distance_count += 1
        if order['is_super_long_distance']:
            super_long_distance_count += 1
    return long_distance_count, super_long_distance_count

def analyze_delivery_boys(orders):
    delivery_boys = {}
    for order in orders:
        if 'delivery_boy' in order and order['delivery_boy']['name']:
            delivery_boy_name = order['delivery_boy']['name']
            if delivery_boy_name in delivery_boys:
                delivery_boys[delivery_boy_name] += 1
            else:
                delivery_boys[delivery_boy_name] = 1
    # Sort delivery boys by count in decreasing order
    sorted_delivery_boys = dict(sorted(delivery_boys.items(), key=lambda item: item[1], reverse=True))
    #unique_delivery_boys_count = len(delivery_boys)
    return sorted_delivery_boys #, unique_delivery_boys_count

def main():
    parser = argparse.ArgumentParser(description="Fetch and Analyze all Swiggy orders.")
    parser.add_argument("session_tid", type=str, help="Session ID for authentication")
    parser.add_argument("output_file", type=str, help="Path to the output CSV file")
    args = parser.parse_args()
    orders = fetch_all_orders(args.session_tid)
    save_analysis_to_csv(orders, args.output_file)


    # Calculate total order amount for each order
    total_order_amounts = [calculate_total_order_amount(order) for order in orders]

    # Calculate average order value
    average_order_value = calculate_average_order_value(orders)

    # Get order frequency
    order_freq = order_frequency(orders)

    # Get monthly order frequency
    monthly_freq = monthly_order_frequency(orders)

    # Get popular items
    popular_items_list = popular_items(orders)

    # Get order status distribution
    status_distribution = order_status_distribution(orders)

    # Get customer analysis data
    customer_data = customer_analysis(orders)

    # Get total discount used
    total_discount_used = discount_usage(orders)

    # Get geographical analysis data
    geographical_data = geographical_analysis(orders)

    # Get peak ordering times
    peak_times = peak_ordering_times(orders)

    # Get geographical Resturant analysis data
    res =geographical_analysis_resturant(orders)

    # Get vegetarian and non-vegetarian item counts
    veg_nonveg_counts = veg_nonveg_analysis(orders)
    
    # Get packing charges analysis
    packing_charges = packing_charges_analysis(orders)

    # Get delivery time analysis
    delivery_times = delivery_time_analysis(orders)

    # Count payment methods
    payment_counts = count_payment_methods(orders)

    # Analyze and print the highest and lowest item prices
    (highest_price_name, highest_price, highest_price_count, 
     lowest_price_name, lowest_price, lowest_price_count) = analyze_item_prices(orders)
    
    # Analyze the Farthest and the nearest distance of resturant & total distance travelled 
    (farthest_name, farthest_distance, farthest_count, 
     nearest_name, nearest_distance, nearest_count, total_distance) = analyze_restaurant_distance(orders)
    
    # Analyze if the order was on rain or not
    rain_count, no_rain_count = analyze_rain_mode(orders)

    # Analyze if the Resturant was long distance or Super long Distance
    long_distance_count, super_long_distance_count = analyze_distance_flags(orders)
    
    # Delivery Guy Name and Count
    delivery_boy_counts = analyze_delivery_boys(orders)
    
    
    
    # Find and print the latest and oldest order dates
    oldest_date, latest_date = find_order_dates(orders)
    print(f"Order From: {oldest_date} to {latest_date} ")

    # Print or use the data as per your requirement
    print("Total order amounts:", sorted(total_order_amounts))
    print("Average order value:", average_order_value)
    print("Order frequency:", dict(order_freq))
    print("Monthly order frequency:", dict(monthly_freq))
    print("Popular items:", popular_items_list)
    print("Order status distribution:", dict(status_distribution))
    print("Customer analysis:", customer_data)
    print("Total discount used:", total_discount_used)
    print("Geographical analysis:", geographical_data)
    print("Peak ordering times:", dict(peak_times))
    print("Resturant Analysis:",dict(res))
    print("Vegetarian and Non-Vegetarian item counts:", veg_nonveg_counts)
    print("Packing charges analysis:", packing_charges)
    print("Delivery time analysis:", delivery_times)
    print("Payment Method Counts:", payment_counts)
    print(f"Highest Item Price: {highest_price} for '{highest_price_name}' ({highest_price_count} times)")
    print(f"Lowest Item Price: {lowest_price} for '{lowest_price_name}' ({lowest_price_count} times)")
    print(f"Farthest Restaurant: {farthest_distance} km to '{farthest_name}' ({farthest_count} times)")
    print(f"Nearest Restaurant: {nearest_distance} km to '{nearest_name}' ({nearest_count} times)")
    print(f"Total Distance Travelled by Delivery Guys: {total_distance} km")
    print(f"Orders in Rain: {rain_count}")
    print(f"Orders not in Rain: {no_rain_count}")
    print(f"Long Distance Orders: {long_distance_count}")
    print(f"Super Long Distance Orders: {super_long_distance_count}")
    #print(f"Total Unique Delivery Boys: {unique_delivery_boys_count}")
    print("Delivery Boys and their Order Counts:")
    for name, count in delivery_boy_counts.items():
        print(f"{name}: {count} orders")




if __name__ == "__main__":
    main()
