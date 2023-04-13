from tkinter import *
import Explain
import Parsers
from database_connection import DBConnection

connection = DBConnection()

def loadInterface():
    #annotate = Annotate()
    #graph = GraphGenerator()
    #Preprocessor1 = Preprocessor()
    #schema = Preprocessor1.get_all_tables_columns()
    #schema = schema["table_name"]
    #schema = schema.values.tolist()
    # Deploy global variables for program's usage
    global root,canvas_1, canvas_2, queryButton,schema_list_box,canvas_input,attribute_list_box,btn_pressed,query_input_1,query_input_2,canvas_annot

    btn_pressed = False
    
    
    # Color Palettes and fonts
    background_color = "#CDDAFD"
    canva_color = "#DFE7FD"
    button_color = "#F0EFEB"
    display_font = "Roboto"

    root = Tk()
    # Title of the program
    root.title("Query Comparison Tool")

    
    # Retrieves the screensize of the monitor
    width = root.winfo_screenwidth()
    height = root.winfo_screenheight()

    # Sets the display size of the GUI
    root.geometry("%dx%d" % (width, height))

    # Set bgColor of GUI
    root.configure(bg = background_color)

    frame_button = Frame()
    
    frame_qep = Frame()
    # Create label for User's Query Input
    query_label_1 = Label(frame_qep, font = (display_font,14), text = "Input Query 1 Here:", bg =background_color )
    query_label_1.grid(row=0,column=0,padx = 40, pady = 10)

    # Canva for Query Input
    canvas_input_1 = Canvas(frame_qep,width = 400, height = 150, borderwidth=2)
    canvas_input_1.grid(row=1,column=0, padx = 10 , pady = 10, sticky=N)

    # Create space for User's Query Input
    query_input_1 = Text(canvas_input_1, width = 58, borderwidth= 2,height = 50, font = (display_font, 10), bg = canva_color, relief = SOLID, wrap= WORD)
    canvas_input_1.create_window(0, 0, window=query_input_1, anchor="nw", tag='input_text', width=408, height=158)

    # Create label for User's Query Input
    query_label_2 = Label(frame_qep, font = (display_font,14), text = "Input Query 2 Here:", bg =background_color )
    query_label_2.grid(row=0,column=1,padx = 40, pady = 10)

    # Canva for Query Input
    canvas_input_2 = Canvas(frame_qep,width = 400, height = 150, borderwidth=2)
    canvas_input_2.grid(row=1,column=1, padx = 10 , pady = 10)

    # Create space for User's Query Input
    query_input_2 = Text(canvas_input_2, width = 58, borderwidth= 2,height = 17, font = (display_font, 10), bg = canva_color, relief = SOLID, wrap= WORD)
    canvas_input_2.create_window(0, 0, window=query_input_2, anchor="nw", tag='input_text',width=408, height=158)

    # Starting Label of Query Annotator
    query_annot_label_1 = Label(frame_qep, font = (display_font, 14), text = "QEP1 Description:",bg = background_color )
    query_annot_label_1.grid(row= 2, column = 0,padx = 40, pady = 10,sticky=N) 

    #Create a canva for text from annotations


    canvas_1 = Text(frame_qep,width = 50, height = 15, borderwidth=2, bg = canva_color, relief = SOLID,wrap= WORD, font = (display_font,12))
    canvas_1.grid(row=2,column=0, padx = 10 , pady = 60)
    # Starting Label of Query Annotator
    query_annot_label_2 = Label(frame_qep, font = (display_font, 14), text = "QEP2 Description:",bg = background_color )
    query_annot_label_2.grid(row=2, column = 1,padx = 40, pady = 10,sticky=N) 

    #Create a canva for text from annotations

    canvas_2 = Text(frame_qep,width = 50, height = 15, borderwidth=2, bg = canva_color, relief = SOLID,wrap= WORD, font = (display_font,12))
    canvas_2.grid(row=2,column=1, padx = 10 , pady = 60)
    
     # Starting Label of Query Plain Explain
    query_explain_label = Label(frame_qep, font = (display_font, 14), text = "Differences and Explainations:",bg = background_color )
    query_explain_label.grid(row= 1, column = 2,padx = 40, pady = 10, sticky=S) 
       #Create a canva for text from annotations

    canvas_annot = Text(frame_qep,width = 50, height = 15, borderwidth=2, bg = canva_color, relief = SOLID,wrap= WORD, font = (display_font,12))
    canvas_annot.grid(row=2,column=2, padx = 10 , pady = 5)
    
    #Setting QEP Frame into grid
    frame_qep.grid(column=2, row=2, columnspan=8, sticky='W')
    frame_qep.configure(bg=background_color)


    # Button to create annotations
    queryButton = Button(frame_button,text = "Genrate QEP Description",bg = canva_color, fg = "black", font = (display_font, 12), relief = RAISED, command = lambda:btn_click())
    queryButton.grid(row=0,column=2,padx = 40,  pady = 10)

    # Button to create visualization
    visualise_button = Button(frame_button, text="Visualise",bg = canva_color, fg = "black", font = (display_font, 12), relief = RAISED, command = lambda:visualize())
    visualise_button.grid(row=1,column=2,padx = 40, pady = 10)
    
    # Button to create visualization
    reset_button = Button(frame_button, text="Reset",bg = canva_color, fg = "black", font = (display_font, 12), relief = RAISED, command = lambda:btn_click_reset())
    reset_button.grid(row=2,column=2,padx = 40, pady = 10, sticky = N)

    # Create to remind users to enter their query...
    frame_button.grid(row=1,column=6,columnspan=8, sticky='W')
    frame_button.configure(bg=background_color)
    
    root.mainloop()

def btn_click():
    global btn_pressed , query_input_1, query_input_2, connection
    
    if not btn_pressed:
        canvas_1.delete("1.0","end")
        canvas_2.delete("1.0","end")
        canvas_annot.delete("1.0", "end")
        sql_query1 = query_input_1.get("1.0", END)
        sql_query2 = query_input_2.get("1.0", END)

        connection = DBConnection()
        QEP1_in_NL = Explain.get_QEP_description(sql_query1, connection)
        QEP2_in_NL = Explain.get_QEP_description(sql_query2, connection)
        difference_and_explanation = Explain.diff_explanation_in_NL(sql_query1, sql_query2, connection)

        canvas_1.insert("1.0", QEP1_in_NL)
        canvas_2.insert("1.0", QEP2_in_NL)
        canvas_annot.insert("1.0", difference_and_explanation)

def btn_click_reset():
    global btn_pressed , query_input_1, query_input_2, connection
    
    if not btn_pressed:
        canvas_1.delete("1.0","end")
        canvas_2.delete("1.0","end")
        canvas_annot.delete("1.0", "end")
        query_input_1.delete("1.0", "end")
        query_input_2.delete("1.0", "end")

def visualize():
    global btn_pressed , query_input_1, query_input_2
    connection = DBConnection()
    if not btn_pressed:
        sql_query_1 = query_input_1.get("1.0", END)
        sql_query_2 = query_input_2.get("1.0", END)
        Explain.plot_tree_graph(sql_query_1, sql_query_2,connection)

if __name__ == "__main__":
    loadInterface()