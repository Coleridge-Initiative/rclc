


class ParsedJsonInterpreter(object):
    
    def __init__(self, object):
        self.object = object
    
    # Parse the JSON and convert it to Txt, devided by sections, and extract the title of the section
    def FindObject(self, object):
        texts = []
        res = []
        titles = []
        for page in self.object['pages']:
            for element in page['elements']:
                try:    
                    if element['type'] == 'heading':
                        title = self.GetText(element)
                        titles.append(title)
                        texts.append(res)
                        res = []
                    if element['type'] in ['word', 'line', 'character', 'paragraph', 'heading', 'list']:
                        res.append(element)
                except TypeError:
                    continue
        texts.append(res)
        return texts[1:], titles
    
    
    def GetText(self, text_object):
        result = ""
        if text_object['type'] in ['paragraph','heading','list']:
            for i in text_object['content']:
                result += self.GetText(i)
        if text_object['type'] in ['line']:
            for i in text_object['content']:
                result += self.GetText(i)
        elif text_object['type'] in ['word']:
            if type(text_object['content']) is list:
                for i in text_object['content']:
                    result += self.GetText(i)
            else:
                result += text_object['content']
                result += ' '
        elif text_object['type'] in ['character']:
            result += text_object['content']
        return result
    
    # Get the text
    def GetSectionalText(self, object):
        text = ""
        sections = []
        text_lists, titles = self.FindObject(object)
        for text_list in text_lists:
            for text_Obj in text_list:
                text += self.GetText(text_Obj)
                text += '\n\n'
            sections.append(text)
            text = ""
        
        return sections, titles
    
    
    
    