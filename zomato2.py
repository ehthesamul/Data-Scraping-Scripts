import requests
import json
from collections import defaultdict
from datetime import datetime
from statistics import mean
import time
import csv


headers = {
    "Host": "www.zomato.com",
    "Cookie": "fbcity=4; fre=0; rd=1380000; zl=en; fbtrack=9d0447ae09e037019ea4b7ab62336d83; ltv=4; lty=4; locus=%7B%22addressId%22%3A0%2C%22lat%22%3A12.971606%2C%22lng%22%3A77.594376%2C%22cityId%22%3A4%2C%22ltv%22%3A4%2C%22lty%22%3A%22city%22%2C%22fetchFromGoogle%22%3Afalse%2C%22dszId%22%3A3655%2C%22fen%22%3A%22Bengaluru%22%7D; _gid=GA1.2.2122818212.1721067708; _gcl_au=1.1.1208508430.1721067708; _fbp=fb.1.1721067709136.526062706458754461; cid=2c4e3ed9-0308-4d16-a237-3a5c99f7e944; G_ENABLED_IDPS=google; ak_bmsc=9E644021992874F6D9805B8C08673544~000000000000000000000000000000~YAAQHkYDF7jPtbaQAQAAzPSvuhi0WJNfkOsiMTX9lC8Ukt/YHQsJYmKmGoVNKXzJoIUpxC3ot3ACBeBpPlTeG9CzB7nS4tvy4rQnO3emRL1ZM6Ykl2JgL9UWB9J+cOXKtTvif5algp3i9Xhu6J0GLa3tMib7pr+Oes+ZkLyBlM3oQj6OMfK/WEXnf4p0om7IbPYcmiiEwrjJU7PwHUrwMDunU8HJnE18abETiWrHR860RhtCf6xGjQTbfAzxvdVDed5ZxOYNEuGM524E3UMVXWdE9REQWDRbTkmk9kvIuzvIydryvqzLu7jB3TFObuLAriLWkzggf+dRhwbfnJN8tgd9NZiH162dhAkdf8U9t2IBOMTlurrqGhb1nrKnD0Sc+W4TKUH1sLY=; _abck=154B4835C2EBFB8A7638694AF48E8D5F~-1~YAAQl7suF9PXrl6QAQAAMgGwugyJ5ByyBOEa7DieNcetQ4gAVKmMLNTLipzY96xPoUOTiU8s5VQMANStplpt0NmiQ7LjblhD9BLT14m+yz5LJBOh6EepADe/rISrNtAimtRU0a+bQqZuw/VEPIihxynkLcLLozbpeVVPvhcAN70kHIk1wZcmZJh000RibyMnYGHO7ozEYaiCLxgYkbJQcKATU13pF5xwRlbTKBjr5O4StA1085levlVNQXsPxG2dNo5+sCjP5dhL6pV/lG89J/5zwVy6KGgwuMQC3am9lkhW1qbjvh723B7CrLyhH0Hs/BI2vfuhebH9pqzl0WYWnv/uakfn71kDrDwL42ppZ8K69fyoTaepZCIf4g5yPUxCGrzT3/dm2pgyWuY+H1I2~-1~-1~-1; bm_sz=6C845C3E2519AC6AD189EEB52FB38980~YAAQl7suF9XXrl6QAQAAMgGwuhiJTM8GB1JSgbFMO3ENndu3M+oRJBDU2aHa3Ha0Ivpx3lL2bBjuCH+qEabXR+AjweLjuui2XLHY5e2aJBFC4ZQiFIOGgSh/Mj+/pADbnyfkTcQ6tvqjACjBM9b7wt+w/RU+iTbYnE2egK6oKaxwBCkHzek9EpZj2njvmgP9NoTZhbuqDMavs2fH5cHAWrmupNYu9S9PVnu9taVe+cf8QpX8XSrO8soefeinCGw8cmaVxfYwOML2Evj3Eq2RUUd5synJN3K0yIrAJqT3S5MSvjoUA9YbTjrD+85AR6AKLltg1KBViUJt/PWAErMruOqqNM1KzyA1x+BFdOys3No4s9lZFg==~3228725~3355449; PHPSESSID=5cdf5677b60e1e099721df71c7bc8dcd; zat=TFynMyeEFDFu_ke82pZUA_A0IzUTgYrCZ4IuucBGC9E.xKNDIcJ5NJ0bb_ty1fDR_XBGwyOq0IdXrIngRzSGfXU; ttaz=1723717972; bm_sv=7336AFA7E7695943DF1111E1A6EB496E~YAAQl7suFyPrrl6QAQAABi4auxi/KvDfcAMCvOvdJdZ+4KH2wYG37FKIEkZXSodrId7DbSeg7+0oX9KTbk+NVbIHM8Cj4/7+6XWMIkThsw1SfZUoQUBO4rlVmdYriZZ5LderI9Wu8n8cSjgCd8hrkQcRXdomVU9rRDM+vsfRTw5ZEYkkYnAh2dQKAU7PWfv8ljHo2rv5r1vr9cwSASv9IC4IMPsQ9ygWJxP9r8neid4gQsjztr9fLDQ3lSeSxzau1Q==~1; csrf=688593e580c80bce40ba3858e8324412; hy-en=1; _gat_global=1; _gat_city=1; _gat_country=1; _ga=GA1.1.789238302.1721067708; _ga_2XVFHLPTVP=GS1.1.1721123219.5.1.1721125987.46.0.0; _ga_ZVRNMB4ZQ5=GS1.2.1721123220.5.1.1721125988.45.0.0; _ga_X6B66E85ZJ=GS1.2.1721123220.5.1.1721125988.45.0.0; AWSALBTG=jACN/Ly9YPmfdkDcGm/TeuL9Qz6jVjJ5Zr61osY3QdEfo0AfT+gYmGm8pbEixWJKj+fkO28RNDN+orajWeq9fo1XQgN4hU7wEBTHaTI2zt6AZzfKOWjCKiHtgCPUFcs9yEz4pNO2NN2w//uIJNDlAixww6EWdoKdXq41ZQdAHRI8; AWSALBTGCORS=jACN/Ly9YPmfdkDcGm/TeuL9Qz6jVjJ5Zr61osY3QdEfo0AfT+gYmGm8pbEixWJKj+fkO28RNDN+orajWeq9fo1XQgN4hU7wEBTHaTI2zt6AZzfKOWjCKiHtgCPUFcs9yEz4pNO2NN2w//uIJNDlAixww6EWdoKdXq41ZQdAHRI8",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.160 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://www.zomato.com/users/<your-user-id>/ordering"
    # Add other headers as needed
}

# Function to fetch orders from Zomato
def fetch_orders():
    orders = []
    page = 1
    total_pages = None

    while True:
        url = f"https://www.zomato.com/webroutes/user/orders?page={page}"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            sections = data.get('sections', {})
            user_order_history = sections.get('SECTION_USER_ORDER_HISTORY', {})
            
            # Extracting total pages from user_order_history
            total_pages = user_order_history.get('totalPages', 0)
            
            if total_pages is None:
                total_pages = data.get("totalPages", 0)
                print(f"Total pages to fetch: {total_pages}")
            
            if "entities" in data:
                for order_id, order_data in data["entities"]["ORDER"].items():
                    orders.append(order_data)
                    print(f"Added order ID {order_id} from page {page}")
                
                page += 1
                if page > total_pages:
                    break
                time.sleep(1)  # Adding a small delay to avoid overwhelming the server
            else:
                break
        else:
            print(f"Failed to fetch orders from page {page}. Status code: {response.status_code}")
            break
    
    return orders
def analyze_orders(orders):
    if not orders:
        print("No order data available.")
        return
    
    total_orders = len(orders)
    total_spend = sum(float(order.get("totalCost", "0").replace("₹", "").replace(",", "")) for order in orders)
    order_dates = [datetime.strptime(order.get("orderDate", ""), "%B %d, %Y at %I:%M %p") for order in orders]
    average_order_value = mean(float(order.get("totalCost", "0").replace("₹", "").replace(",", "")) for order in orders)
    total_discount = sum(float(order.get("discountAmount", "0").replace("₹", "").replace(",", "")) for order in orders)
    
    # Calculate frequency by date
    date_counts = defaultdict(int)
    for date in order_dates:
        date_counts[date.date()] += 1
    
    # Calculate monthly frequency
    monthly_counts = defaultdict(int)
    for date in order_dates:
        monthly_counts[date.strftime("%Y-%m")] += 1
    
    # Calculate popular items count
    item_counts = defaultdict(int)
    for order in orders:
        dishes = order.get("dishString", "").split(",")
        for dish in dishes:
            item_counts[dish.strip()] += 1
    
    # Calculate restaurant order count and rating stats
    restaurant_counts = defaultdict(lambda: {"count": 0, "rating": 0.0, "votes": 0})
    for order in orders:
        restaurant_info = order.get("resInfo", {})
        restaurant_name = restaurant_info.get("name", "")
        if restaurant_name:
            restaurant_counts[restaurant_name]["count"] += 1
            rating = restaurant_info.get("rating", {}).get("aggregate_rating", "0")
            votes = restaurant_info.get("rating", {}).get("votes", "0").replace(",", "")
            restaurant_counts[restaurant_name]["rating"] = rating
            restaurant_counts[restaurant_name]["votes"] = votes
    
    # Calculate order frequency by hour
    hour_counts = defaultdict(int)
    for order in orders:
        order_time = order.get("orderDate", "")
        if order_time:
            try:
                order_hour = datetime.strptime(order_time.split(" at ")[1], "%I:%M %p").strftime("%H")
                hour_counts[order_hour] += 1
            except ValueError:
                pass  # Handle cases where order_time format does not match
    
    # Calculate geographical analysis of delivery addresses
    address_counts = defaultdict(int)
    for order in orders:
        delivery_address = order.get("deliveryDetails", {}).get("deliveryAddress", "")
        if delivery_address:
            address_counts[delivery_address] += 1
    # Calculate delivery label count
    delivery_label_counts = defaultdict(int)
    for order in orders:
        delivery_label = order.get("deliveryDetails", {}).get("deliveryLabel", "")
        if delivery_label:
            delivery_label_counts[delivery_label] += 1
    
    # Example output (replace with actual analysis results):
    print(f"Total Orders: {total_orders}")
    print(f"Average Order Value: ₹{average_order_value:.2f}")
    print(f"Total Spend: ₹{total_spend:.2f}")
    print(f"Total Discount: ₹{total_discount:.2f}")
    print("Order Frequency by Date:")
    for date, count in sorted(date_counts.items()):
        print(f"{date}: {count} orders")
    print("Monthly Order Frequency:")
    for month, count in sorted(monthly_counts.items()):
        print(f"{month}: {count} orders")
    print("Popular Items Count:")
    for item, count in sorted(item_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"{item}: {count} orders")
    print("Restaurant Order Count and Rating Stats:")
    for restaurant, stats in sorted(restaurant_counts.items(), key=lambda x: x[1]["count"], reverse=True):
        print(f"{restaurant}: {stats['count']} orders, Rating: {stats['rating']}, Votes: {stats['votes']}")
    print("Order Frequency by Hour:")
    for hour, count in sorted(hour_counts.items()):
        print(f"{hour}:00 - {(int(hour) + 1) % 24}:00: {count} orders")
    print("Geographical Analysis of Delivery Addresses:")
    for address, count in sorted(address_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"{address}: {count} orders")
    print("Delivery Label Count:")
    for label, count in sorted(delivery_label_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"{label}: {count} orders")
    

    csv_file = "zomato_orders_analysis.csv"
    with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        
        writer.writerow(["Metric", "Value"])
        writer.writerow(["Total Orders", total_orders])
        writer.writerow(["Average Order Value", f"₹{average_order_value:.2f}"])
        writer.writerow(["Total Spend", f"₹{total_spend:.2f}"])
        writer.writerow(["Total Discount", f"₹{total_discount:.2f}"])
        writer.writerow([])  # Blank row for separation
        writer.writerow(["Order Frequency by Date"])
        writer.writerow(["Date", "Count"])
        for date, count in sorted(date_counts.items()):
            writer.writerow([date, count])
        writer.writerow([])  # Blank row for separation
        writer.writerow(["Monthly Order Frequency"])
        writer.writerow(["Month", "Count"])
        for month, count in sorted(monthly_counts.items()):
            writer.writerow([month, count])
        writer.writerow([])  # Blank row for separation
        writer.writerow(["Popular Items Count"])
        writer.writerow(["Item", "Count"])
        for item, count in sorted(item_counts.items(), key=lambda x: x[1], reverse=True):
            writer.writerow([item, count])
        writer.writerow([])  # Blank row for separation
        writer.writerow(["Restaurant Order Count and Rating Stats"])
        writer.writerow(["Restaurant", "Count", "Rating", "Votes"])
        for restaurant, stats in sorted(restaurant_counts.items(), key=lambda x: x[1]["count"], reverse=True):
            writer.writerow([restaurant, stats["count"], stats["rating"], stats["votes"]])
        writer.writerow([])  # Blank row for separation
        writer.writerow(["Order Frequency by Hour"])
        writer.writerow(["Hour Range", "Count"])
        for hour, count in sorted(hour_counts.items()):
            writer.writerow([f"{hour}:00 - {(int(hour) + 1) % 24}:00", count])
        writer.writerow([])  # Blank row for separation
        writer.writerow(["Geographical Analysis of Delivery Addresses"])
        writer.writerow(["Address", "Count"])
        for address, count in sorted(address_counts.items(), key=lambda x: x[1], reverse=True):
            writer.writerow([address, count])
        writer.writerow([])  # Blank row for separation
        writer.writerow(["Delivery Label Count"])
        writer.writerow(["Label", "Count"])
        for label, count in sorted(delivery_label_counts.items(), key=lambda x: x[1], reverse=True):
            writer.writerow([label, count])
    
    print(f"Analysis results saved to {csv_file}")

def main():
    # Fetch all orders
    all_orders = fetch_orders()
    if all_orders:
        analyze_orders(all_orders)

    # Save orders to a JSON file
    with open("zomato_orders.json", "w", encoding="utf-8") as file:
        json.dump(all_orders, file, ensure_ascii=False, indent=4)

    print(f"Total orders fetched: {len(all_orders)}")

if __name__ == "__main__":
    main()
