import argparse
import time
from core.database import DBManager
from tasks import process_company
import sys

def main():
    parser = argparse.ArgumentParser(description="AI Data Extraction Pipeline - Distributed Celery Master")
    parser.add_argument('--ingest', type=str, help='Path to Excel file to load into the PostgreSQL Database')
    parser.add_argument('--process', type=int, help='Queue N pending records into the Redis Broker', metavar='N')
    parser.add_argument('--export', type=str, help='Path to export the final matched Excel file')

    args = parser.parse_args()
    db = DBManager()

    if args.ingest:
        print(f"Ingesting {args.ingest} into PostgreSQL Database...")
        db.load_from_excel(args.ingest)
        print("Ingestion complete. You can now dispatch jobs with --process <N>.")

    if args.process is not None:
        limit = args.process
        print(f"Fetching up to {limit} pending companies from PostgreSQL...")
        pending = db.get_pending_batch(limit=limit)
        
        if not pending:
            print("No pending records found to process.")
        else:
            print(f"Dispatching {len(pending)} jobs to the Celery Redis Queue...")
            for comp in pending:
                process_company.delay(comp['id'])
                
            print(f"All {len(pending)} tasks successfully dispatched to Redis Queue!")
            
            print("\n" + "="*50)
            print("🚀 BATCH PROCESSING STARTED 🚀".center(50))
            print("="*50)
            
            start_time = time.time()
            
            # Start a Live Polling Loop
            while True:
                queued = db.get_queued_count()
                if queued == 0:
                    break
                
                # Print a clean overwriting progress line
                sys.stdout.write(f"\r⏳ Waiting for Celery Drones to finish... ({queued} remaining in queue)")
                sys.stdout.flush()
                time.sleep(2)  # Check every 2 seconds
                
            elapsed = time.time() - start_time
            total_scanned, total_matches = db.get_stats()
            
            print(f"\r⏳ Waiting for Celery Drones to finish... (0 remaining in queue)  \n")
            
            print("="*50)
            print("🎉 BATCH SUCCESSFULLY COMPLETED 🎉".center(50))
            print("="*50)
            print(f"⏱️  Time Taken:           {elapsed:.2f} seconds")
            print(f"📦 Total DB Scanned:     {total_scanned} companies")
            print(f"🎯 Total DB True Hits:   {total_matches} companies")
            print("="*50 + "\n")

    if args.export:
        print(f"Exporting successfully matched companies to {args.export}...")
        db.export_success_to_excel(args.export)
        
        total_done, total_matches = db.get_stats()
        print("\n" + "="*50)
        print("🎉 EXPORT COMPLETE 🎉".center(50))
        print("="*50)
        print(f"📦 Total Companies Scanned:        {total_done}")
        print(f"🎯 Total Output Matches Found:     {total_matches}")
        print("="*50 + "\n")

if __name__ == "__main__":
    main()
