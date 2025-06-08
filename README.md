# BTL CSDLPT

## Yêu Cầu
- Python 3.12.3
- OS: Ubuntu 24.04.2 LTS x86_64 

## Cài đặt
1. Tạo môi trường ảo:
   ```bash
   python3 -m venv .venv
   source ./.venv/bin/activate
2. Cài đặt thư viện:
   ```bash
   pip3 install -r requirements.txt 

3. Tải file ratings.dat 

4. Cấu hình .env
   ```bash
   HOST=localhost
   PORT=5432
   DATABASE_NAME=your_database_name
   USER=your_username
   PASSWORD=your_password
   ```
   DATABASE_NAME: là tên của database lưu trữ bảng ratings và các phân mảnh của nó. 
   Ví dụ: dds_assgn1
- Nếu gặp lỗi PostgreSQL: 
[Xử lý lỗi peer authentication cho postgresql](https://stackoverflow.com/questions/18664074/getting-error-peer-authentication-failed-for-user-postgres-when-trying-to-ge)
