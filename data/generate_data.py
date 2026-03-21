import pandas as pd, numpy as np, random
from faker import Faker
fake = Faker()

def generate_orders(n=50000):
    df = pd.DataFrame({
        'order_id':    [f'ORD-{i:06d}' for i in range(n)],
        'customer_id': [f'CUST-{random.randint(1,5000):04d}' for _ in range(n)],
        'product_sku': [random.choice(['SKU-A1','SKU-B2','SKU-C3',None]) for _ in range(n)],
        'order_date':  pd.date_range('2023-01-01', periods=n, freq='2min'),
        'revenue':     np.random.choice(
                           list(np.random.uniform(5, 500, n-50)) + [-99.99]*50
                       ),  # inject bad negatives
        'region':      [random.choice(['East','West','Central',None]) for _ in range(n)],
        'status':      [random.choice(['completed','pending','COMPLETED']) for _ in range(n)]
    })
    # inject ~2% duplicate order_ids (common pipeline bug)
    dups = df.sample(frac=0.02)
    return pd.concat([df, dups]).sample(frac=1).reset_index(drop=True)

generate_orders().to_csv('data/orders_raw.csv', index=False)