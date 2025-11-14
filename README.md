# Adobe AA Weekly DMA Report (FTP only)

Pulls Adobe Analytics 2.0 **DMA × Last Touch Channel (Segments)** per RSID every **Monday at 10:00 AM ET** for the prior **Sunday→Saturday** week, uploads the Excel to an **FTP** site (ftp.omniture.com), and stores a workflow artifact. **No email step.**

## Output columns (exact order)
`Date, DMA, Last_Touch_Channel, Visits, Orders, Demand, NMA, NTF`

## One-time setup

1. **Files in repo:**
   - `aa_dma_by_segment.py`
   - `requirements.txt`
   - `.gitignore`
   - `.github/workflows/aa-weekly.yml`
   - `README.md`

2. **GitHub Actions Secrets** (Repo → Settings → Secrets and variables → Actions):
   - **Adobe**  
     - `AA_CLIENT_ID`  
     - `AA_CLIENT_SECRET`  
     - `AA_API_KEY` *(often same as client id)*  
     - `AA_ORG_ID`  
     - `AA_COMPANY_ID`
   - **FTP (Adobe Omniture)**  
     - `FTP_USER` = `Products`  
     - `FTP_PASS` = `I*nePMUz`  
     *(Host is set to `ftp.omniture.com` in the workflow. To change remote path, edit `FTP_DIR`.)*

3. **(Optional) Add more RSIDs**
   - In the workflow’s “Run report” step `env`, set:
     ```
     BRAND_RSIDS: vrs_ospgro1_womanwithin,vrs_ospgro1_jessicalondon,vrs_ospgro1_ellos
     ```

4. **Manual run (test any time):**  
   GitHub → **Actions** → *Adobe AA Weekly DMA Report (FTP only)* → **Run workflow**.

The Excel is written to `output/adobe_dma_by_segment_YYYY-MM-DD_to_YYYY-MM-DD.xlsx`, then uploaded to `ftp.omniture.com` (to `FTP_DIR`), and also saved as a build artifact.
