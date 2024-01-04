from PyPDF2 import PdfWriter, PdfReader

filepath = ("/Users/pongabha/Library/CloudStorage/GoogleDrive-pabhakara@bearcat.co.th/Shared "
            "drives/Resources/Training/IFPD/PANS-OPS Recurrent Nov 13-17, 2023/")

filename = "Recurrent IFP 2023 Aerothai"

inputpdf = PdfReader(open(f"{filepath}{filename}.pdf", "rb"))

participant_list = ['Pongabha','Woraphan','Thanawadee','Samaporn','Thagoon','Ungsumalee','Thapana',
                    'Nichakun','Thanyarat','Panatta','Alongkorn','Thaikhom','Khanin','Juthatip',
                    'Suparin','Wissinee','Putti','Tawika','Pawat','Chai','Karan','Harit']

for i in range(len(inputpdf.pages)):
    output = PdfWriter()
    output.add_page(inputpdf.pages[i])
    with open(f"{filepath}PANS-OPS Recurrent-Refresher 2023_{participant_list[i]}.pdf", "wb") as outputStream:
        output.write(outputStream)