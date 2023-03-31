import pdfplumber
from openpyxl import Workbook
from tqdm import tqdm
def extract_table_from_pdf(pdf_path):
    with pdfplumber.open(pdf_path) as pdf_file:
        # 实例化一个工作簿对象
        wb = Workbook()
        # 获取第一个工作表
        ws = wb.active
        for i, page in tqdm(enumerate(pdf_file.pages), desc="正在读取PDF文件", total=len(pdf_file.pages)):
            # 查找表格
            tables = page.extract_tables()
            if tables:
                # 如果找到表格则将其写入Excel文件
                for table in tables:
                    # 将表格中的每一行写入Excel文件
                    for row in table:
                        row_list = [str(cell).strip() for cell in row]
                        ws.append(row_list)
        # 将工作簿保存为Excel文件
        wb.save(pdf_path.replace(".pdf", ".xlsx"))
        return True
if __name__ == "__main__":
    # 输入pdf文件路径，例如：C:\Users\example.pdf
    pdf_path = input("请输入PDF文件路径：")
    extract_table_from_pdf(pdf_path)
    print(f"成功将表格数据保存到 {pdf_path.replace('.pdf', '.xlsx')}。")