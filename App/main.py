import job_iasi_metop_a
import job_iasi_metop_b
import job_iasi_metop_c
import job_airs
import job_tanso2_fts_srfp

def main():
    print("Starting data retrieval process...")
    
    job_iasi_metop_a.download_data()
    job_iasi_metop_b.download_data()
    job_iasi_metop_c.download_data()
    job_airs.download_data()
    job_tanso2_fts_srfp.download_data()
    
    print("Data retrieval process completed.")

if __name__ == "__main__":
    main()
