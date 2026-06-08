# data_generator/generate_providers.py
# Generates synthetic provider data scattered across two "source systems"
# with deliberate messiness (typos, format differences, missing fields),
# so our pipeline has a realistic entity-resolution problem to solve.

import csv
import random
import os
from faker import Faker

fake = Faker("en_US")
Faker.seed(42)      # fixed seed = same data every run, so results are reproducible
random.seed(42)

# Real medical specialties, so the data looks authentic
SPECIALTIES = ["Cardiology", "Family Medicine", "Pediatrics",
               "Orthopedic Surgery", "Dermatology", "Internal Medicine"]


def make_npi():
    """NPI = National Provider Identifier, the real 10-digit ID every US
    provider has. We generate fake 10-digit numbers in the same format."""
    return str(random.randint(1000000000, 9999999999))


def messy_name(first, last):
    """Returns a randomly 'messed up' version of a name, mimicking how
    different systems record the same person inconsistently."""
    variants = [
        f"{first} {last}",                 # clean
        f"{first} {last}, MD",             # with credential suffix
        f"Dr. {first} {last}",             # with title
        f"{first[0]}. {last}",             # initial instead of first name
        f"{first} {last} ",                # trailing space (common dirty data)
    ]
    return random.choice(variants)


def messy_address(addr):
    """Simulates address format differences between systems."""
    variants = [
        addr,
        addr.replace("Street", "St").replace("Avenue", "Ave"),
        addr.upper(),
    ]
    return random.choice(variants)


def generate(num_providers=300):
    """Creates a pool of 'true' providers, then writes each one into two
    source-system files in messy, inconsistent ways. Some providers appear
    in both systems (duplicates to resolve); some in only one."""

    # 1. Build the pool of real underlying providers
    providers = []
    for _ in range(num_providers):
        first = fake.first_name()
        last = fake.last_name()
        providers.append({
            "npi": make_npi(),
            "first": first,
            "last": last,
            "specialty": random.choice(SPECIALTIES),
            "address": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "zip": fake.zipcode(),
            # license expiry: some already expired (stale records to flag later)
            "license_expiry": fake.date_between(
                start_date="-1y", end_date="+3y").isoformat(),
        })

    # 2. Scatter providers into two source systems with messiness
    system_a = []   # e.g., the credentialing system
    system_b = []   # e.g., the claims system

    for p in providers:
        # ~70% of providers appear in System A
        if random.random() < 0.70:
            system_a.append({
                "source_id": f"A-{random.randint(10000, 99999)}",
                "npi": p["npi"] if random.random() < 0.85 else "",  # 15% missing NPI
                "provider_name": messy_name(p["first"], p["last"]),
                "specialty": p["specialty"],
                "address": messy_address(p["address"]),
                "city": p["city"],
                "state": p["state"],
                "zip": p["zip"],
                "license_expiry": p["license_expiry"],
            })
        # ~70% appear in System B (overlap creates the duplicates to resolve)
        if random.random() < 0.70:
            system_b.append({
                "source_id": f"B-{random.randint(10000, 99999)}",
                "npi": p["npi"] if random.random() < 0.90 else "",
                "provider_name": messy_name(p["first"], p["last"]),
                "specialty": p["specialty"],
                "address": messy_address(p["address"]),
                "city": p["city"],
                "state": p["state"],
                "zip": p["zip"],
                "license_expiry": p["license_expiry"],
            })

    # 3. Write each system to its own CSV file in a local 'raw_data' folder
    os.makedirs("raw_data", exist_ok=True)
    _write_csv("raw_data/system_a_providers.csv", system_a)
    _write_csv("raw_data/system_b_providers.csv", system_b)

    print(f"Generated {len(providers)} underlying providers.")
    print(f"System A file: {len(system_a)} records")
    print(f"System B file: {len(system_b)} records")
    print(f"(Overlap between systems = the duplicates our pipeline will resolve.)")


def _write_csv(path, rows):
    """Helper: writes a list of dictionaries to a CSV file."""
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    generate(num_providers=300)