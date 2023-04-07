from tkinter import *
from preprocessing import Preprocessor
from annotation import Annotate, GraphGenerator

def loadInterface():
    annotate = Annotate()
    graph = GraphGenerator()
    Preprocessor1 = Preprocessor()
    schema = Preprocessor1.get_all_tables_columns()
    schema = schema["table_name"]
    schema = schema.values.tolist()
    # Deploy global variables for program's usage
    global root,canvas,queryButton,schema_list_box,canvas_input,attribute_list_box,btn_pressed,query_input,canvas_annot

    btn_pressed = False
    
    
    # Color Palettes and fonts
    background_color = "#CDDAFD"
    canva_color = "#DFE7FD"
    button_color = "#F0EFEB"
    display_font = "Roboto"

    root = Tk()
    # Title of the program
    root.title("Query Annotater Program")

    
    # Retrieves the screensize of the monitor
    width = root.winfo_screenwidth()
    height = root.winfo_screenheight()

    # Sets the display size of the GUI
    root.geometry("%dx%d" % (width, height))

    # Set bgColor of GUI
    root.configure(bg = background_color)

    # |Schemal Table| Query Input | 
    # |Listbox displays| Query Annotator | Create Annotation! 
    #Create frame schema
    frame_schema = Frame()
    frame_button = Frame()
    # Schema Label
    schema_label = Label(frame_schema, font = (display_font, 14), text = "Schemas Table:",bg = background_color)
    schema_label.grid(row=0,column=0, padx = 20, pady = 10)
    
     # Attribute List
    attribute_list = Label(frame_schema, font = (display_font, 14), text = "Attributes:",bg = background_color)
    attribute_list.grid(row=0,column=1, padx = 20, pady = 10)

    # Creates a block of list for the different schemas

    schema_list_box = Listbox(frame_schema,height = 15, width = 50, bg = canva_color, relief = SOLID, font = (display_font, 12))
    
    for idx, val in enumerate(schema):
        schema_list_box.insert(idx, val)
    schema_list_box.grid(row=1 ,column = 0, padx = 20, pady = 10)
    schema_list_box.select_set(0)
    
    schema_list_box.bind('<<ListboxSelect>>', on_selection)
    
    # Creates a block of list for the different schema's attributes
    attribute_list_box = Listbox(frame_schema,height = 15, width = 50, bg = canva_color, relief = SOLID, font = (display_font, 12))
    attribute_list_box.grid(row=1 ,column = 1, padx = 20, pady = 10)
    
    # Set Schema Frame into grid
    frame_schema.grid(column=1, row=1, columnspan=4, sticky='W')
    frame_schema.configure(bg=background_color)
    
    frame_qep = Frame()
    # Create label for User's Query Input
    query_label = Label(frame_qep, font = (display_font,14), text = "Input Query Here:", bg =background_color )
    query_label.grid(row=2,column=0,padx = 40, pady = 10)

    # Canva for Query Input
    canvas_input = Canvas(frame_qep,width = 450, height = 270, borderwidth=2)
    canvas_input.grid(row=3,column=0, padx = 10 , pady = 10)

    # Create space for User's Query Input
    query_input = Text(canvas_input, width = 65, borderwidth= 2,height = 17, font = (display_font, 10), bg = canva_color, relief = SOLID, wrap= WORD)
    canvas_input.create_window(0, 0, window=query_input, anchor="nw", tag='input_text')

    # Starting Label of Query Annotator
    query_annot_label = Label(frame_qep, font = (display_font, 14), text = "Query Annotator:",bg = background_color )
    query_annot_label.grid(row= 2, column = 1,padx = 40, pady = 10) 

    #Create a canva for text from annotations

    canvas = Text(frame_qep,width = 50, height = 15, borderwidth=2, bg = canva_color, relief = SOLID,wrap= WORD, font = (display_font,12))
    canvas.grid(row=3,column=1, padx = 10 , pady = 5)
    
    #Create a canva for text from annotations

    canvas_annot = Text(frame_qep,width = 50, height = 15, borderwidth=2, bg = canva_color, relief = SOLID,wrap= WORD, font = (display_font,12))
    canvas_annot.grid(row=3,column=2, padx = 10 , pady = 5)
    
     # Starting Label of Query Plain Explain
    query_explain_label = Label(frame_qep, font = (display_font, 14), text = "Comparison Explaination:",bg = background_color )
    query_explain_label.grid(row= 2, column = 2,padx = 40, pady = 10) 
    
    #Setting QEP Frame into grid
    frame_qep.grid(column=2, row=2, columnspan=8, sticky='W')
    frame_qep.configure(bg=background_color)


    # Button to create annotations
    queryButton = Button(frame_button,text = "Create Annotation!",bg = canva_color, fg = "black", font = (display_font, 12), relief = RAISED, command = lambda:btn_click(annotate))
    queryButton.grid(row=0,column=2,padx = 40,  pady = 10)

    # Button to create visualization
    visualise_button = Button(frame_button, text="Visualise",bg = canva_color, fg = "black", font = (display_font, 12), relief = RAISED, command = lambda:visualize(graph))
    visualise_button.grid(row=1,column=2,padx = 40, pady = 10)
    
    # Button to create visualization
    reset_button = Button(frame_button, text="Reset",bg = canva_color, fg = "black", font = (display_font, 12), relief = RAISED, command = lambda:btn_click_reset())
    reset_button.grid(row=2,column=2,padx = 40, pady = 10, sticky = N)

    # Create to remind users to enter their query...
    qep_label = Label(frame_qep, font = (display_font,18), text = "Please Enter a Query and Click Annotate! \nClick On Visualise to see the QEP Graph :)", 
                    bg = background_color, fg = "red")
    qep_label.grid(row=4,column = 1,padx = 10,  pady = 10,sticky = SE)
    frame_button.grid(row=1,column=6,columnspan=8, sticky='W')
    frame_button.configure(bg=background_color)
    
    root.mainloop()
    
# Allows selection of schema which will then transfer the schema's attribute onto another listbox.
def on_selection(event):
    current_id = 0
    print('previous:', schema_list_box.get('active'))
    current_item = schema_list_box.get(schema_list_box.curselection())
    print(' current:', current_item)
    print('(event) previous:', event.widget.get('active'))
    print('(event)  current:', event.widget.get(event.widget.curselection()))
    
    Preprocessor1 = Preprocessor()
    df = Preprocessor1.get_all_tables_columns()
    
    for idx,x in enumerate(df["table_name"]):
        print(x)
        if (x== current_item):
            current_id = idx
            break
    print(f"this is id number {current_id}")
    for idx,x in enumerate(df["table_columns"]):
        print(idx,x)

    attribute_list = df["table_columns"][current_id]
    attribute_list_box.delete(0,END)
    
    for idx,x in enumerate(attribute_list):
        attribute_list_box.insert(idx,x)

def btn_click(annotate):
    global btn_pressed , query_input
    
    if not btn_pressed:
        canvas.delete("1.0","end")
        canvas_annot.delete("1.0", "end")
        sql_query = query_input.get("1.0", END)
        annotatedPlan = annotate.explainQEP(sql_query)
        annotatedPlanExplain = annotate.comparePlans(sql_query)
        print(annotatedPlanExplain)
        canvas.insert("1.0", annotatedPlan)
        canvas_annot.insert("1.0", annotatedPlanExplain)

def btn_click_reset():
    global btn_pressed , query_input
    
    if not btn_pressed:
        canvas.delete("1.0","end")
        canvas_annot.delete("1.0", "end")
        query_input.delete("1.0", "end")

def visualize(graph):
    global btn_pressed , query_input

    if not btn_pressed:
        sql_query = query_input.get("1.0", END)
        graph.generate(sql_query)
