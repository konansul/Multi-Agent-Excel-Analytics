from backend.app.ingestion.dataset_loader import load_from_path

if __name__ == '__main__':
    sheets = load_from_path('../test_data/data_jobs_salary_monthly.xlsx')

    for s in sheets:
        print(f'File: {s.file_name}, sheet: {s.sheet_name}, shape: {s.shape}')