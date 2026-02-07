import argparse
import importlib
import os
import sys


def main():
    # 1. Setup Command Line Arguments
    parser = argparse.ArgumentParser(description="Aviation Data Project Job Runner")

    # Required args
    parser.add_argument(
        "--job",
        required=True,
        help="The name of the job module to run (e.g., jobs.05_data_skew.5_4_salting_optimization)",
    )

    # Optional args
    parser.add_argument(
        "--partitions",
        type=str,
        default="13",
        help="Number of Spark shuffle partitions (default: 13)",
    )

    # Optional args
    parser.add_argument(
        "--salt",
        type=str,
        default="20",
        help="Salting factor for skew optimization",
    )

    args = parser.parse_args()

    # Store the partition & salt count into environment variable temporarily
    os.environ["SPARK_SHUFFLE_PARTITIONS"] = args.partitions
    os.environ["SALT_FACTOR"] = args.salt

    # 2. Construct the full module path
    # Example: src.jobs.05_data_skew.5_4_salting_optimization
    module_path = f"src.{args.job}"

    try:
        print("=" * 60)
        print(f"🚀 Launching Job: {module_path}")
        print(f"⚙️  Configs -> Partitions: {args.partitions}, Salt Factor: {args.salt}")
        print("-" * 60)

        # 3. Dynamic Import
        job_module = importlib.import_module(module_path)

        # 4. Execute the main() function of the job
        if hasattr(job_module, "main"):
            job_module.main()

            print("-" * 60)
            print(f"✅ Job '{args.job}' completed successfully.")
        else:
            print(f"❌ Error: {module_path} does not have a main() function.")

    except ImportError as e:
        print(f"❌ Module not found: {module_path}")
        print(f"Details: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"🔥 Unexpected error during execution: {e}")
        sys.exit(1)  # shut down the program when the error exists


if __name__ == "__main__":
    main()
