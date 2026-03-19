import pypff

pst = pypff.file()
pst.open("your_file.pst")   # ← change this path

root = pst.get_root_folder()

# Walk to the first real message
def find_first_message(folder):
    for i in range(folder.number_of_sub_messages):
        return folder.get_sub_message(i)
    for j in range(folder.number_of_sub_folders):
        msg = find_first_message(folder.get_sub_folder(j))
        if msg:
            return msg

msg = find_first_message(root)

print("subject        :", repr(msg.subject))
print("sender_name    :", repr(msg.sender_name))
print("plain_text_body:", repr(msg.plain_text_body)[:200] if msg.plain_text_body else None)
print("html_body      :", repr(msg.html_body)[:200] if msg.html_body else None)
print("delivery_time  :", repr(msg.delivery_time))
print("client_submit  :", repr(getattr(msg, "client_submit_time", "ATTR_MISSING")))
print("transport_hdr  :", repr(msg.transport_headers)[:300] if msg.transport_headers else None)
print("num_recipients :", msg.number_of_recipients)
if msg.number_of_recipients > 0:
    r = msg.get_recipient(0)
    print("  recip[0] display_name :", repr(r.display_name))
    print("  recip[0] email_address:", repr(r.email_address))
