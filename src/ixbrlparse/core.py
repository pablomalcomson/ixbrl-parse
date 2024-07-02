from pathlib import Path
import sys
from typing import IO, Dict, Generator, Iterable, List, Optional, Union

from bs4 import BeautifulSoup, Tag

from ixbrlparse.components import ixbrlContext, ixbrlNonNumeric, ixbrlNumeric
from ixbrlparse.components._base import ixbrlError

FILETYPE_IXBRL = "ixbrl"
FILETYPE_XBRL = "xbrl"


class IXBRLParseError(Exception):
    pass











class BaseParser:
    def _get_tag_attribute(
        self, s: Union[BeautifulSoup, Tag], tag: Union[str, List[str]], attribute: str
    ) -> Optional[str]:
        tag_contents = s.find(tag)
        if isinstance(tag_contents, Tag):
            attribute_value = tag_contents.get(attribute)
            if isinstance(attribute_value, str):
                return attribute_value.strip()
        return None  # pragma: no cover
    
    # The `_get_tag_attribute` method finds and retrieves the value of a specified attribute from 
    # the first occurrence of a given tag within a `BeautifulSoup` or `Tag` object. It checks if 
    # the found tag is indeed a `Tag` object and whether the attribute value is a string. If so, 
    # it returns the attribute value stripped of any leading or trailing whitespace; otherwise, it 
    # returns `None`.

    def _get_tag_text(self, s: Union[BeautifulSoup, Tag], tag: Union[str, List[str]]) -> Optional[str]:
        tag_contents = s.find(tag)
        if isinstance(tag_contents, Tag):
            text_value = tag_contents.text
            if isinstance(text_value, str):
                return text_value.strip()
        return None  # pragma: no cover
    
    # The `_get_tag_text` method retrieves the text content of the first occurrence of a specified tag 
    # within a `BeautifulSoup` or `Tag` object. It checks if the found tag is a `Tag` object and whether 
    # the text content is a string. If so, it returns the text content stripped of any leading or 
    # trailing whitespace; otherwise, it returns `None`.

    def _get_tag_children(self, s: Union[BeautifulSoup, Tag], tag: Union[str, List[str]]) -> Iterable[Tag]:
        tag_contents = s.find(tag)
        if isinstance(tag_contents, Tag):
            return tag_contents.findChildren()
        return []
    
    # The `_get_tag_children` method retrieves all child tags of the first occurrence of a specified tag 
    # within a `BeautifulSoup` or `Tag` object. If the found tag is a `Tag` object, it returns its children; 
    # otherwise, it returns an empty list.

    def _get_schema(self) -> None:
        pass

    def _get_contexts(self) -> None:
        pass

    def _get_units(self) -> None:
        pass

    def _get_nonnumeric(self) -> None:
        pass

    def _get_numeric(self) -> None:
        pass

    # These methods establish a contract for subclasses, ensuring that any class inheriting from `BaseParser` 
    # will have methods for parsing schema, contexts, units, non-numeric, and numeric data. 












class IXBRLParser(BaseParser):
    root_element: str = "html"

    def __init__(self, soup: BeautifulSoup, raise_on_error: bool = True) -> None:  # noqa: FBT001, FBT002
        self.soup = soup
        self.raise_on_error = raise_on_error
        self.errors: List = []
        self.contexts: Dict[str, ixbrlContext] = {}
        self.schema: Optional[str] = None
        self.namespaces: Dict[str, Union[str, List[str]]] = {}
        self.nonnumeric: List[ixbrlNonNumeric] = []
        self.numeric: List[ixbrlNumeric] = []

    # The `IXBRLParser` class, inheriting from `BaseParser`, is designed to parse iXBRL documents. It initializes 
    # with a `BeautifulSoup` object and an optional error-handling flag. The class sets up attributes to store 
    # parsed data: a list for errors, dictionaries for contexts and namespaces, an optional schema string, and 
    # lists for non-numeric and numeric data elements.
        


    def _get_schema(self) -> None:

# new method named `_get_schema` which returns `None`.

        self.schema = None

# set the instance attribute `schema` to `None`.

        schema_tag = self.soup.find(["link:schemaRef", "schemaRef", "link:schemaref", "schemaref"])

# find any of the specified schema reference tags in the `soup` object and assign the first occurrence to `schema_tag`.

        if isinstance(schema_tag, Tag) and schema_tag.get("xlink:href"):

# check if the found tag is a `Tag` and has the `xlink:href` attribute.

            schema = schema_tag["xlink:href"]

# if yes, retrieve the value of the `xlink:href` attribute from `schema_tag` and assign it to `schema`.

            if isinstance(schema, str):

# check if `schema` is a string:

                self.schema = schema.strip()

# if yes, assign the stripped `schema` value to the instance attribute `schema`.

        self.namespaces = {}

# set the instance attribute `namespaces` to an empty dictionary.

        namespace_tag = self.soup.find(self.root_element)

# search for the root element tag in the `soup` object and assigns it to `namespace_tag`.

        if isinstance(namespace_tag, Tag):

# check if the `namespace_tag` is a `Tag` object.

            for k in namespace_tag.attrs:

# if yes, start a loop that iterates over each attribute `k` in the `namespace_tag`.

                if isinstance(k, str) and (k.startswith("xmlns") or ":" in k):

# check if the attribute name `k` is a string and either starts with "xmlns" or contains ":".

                    namespace_value = namespace_tag[k]

                    # print(f"namespace_value: {namespace_value}", file=sys.stderr)

# if yes, retrieve the value of the attribute `k` from `namespace_tag` and assign it to `namespace_value`.

                    if isinstance(namespace_value, str):

# check if `namespace_value` is a string.

                        self.namespaces[k] = namespace_value.split(" ")

                        # print(f"X: {self.namespaces}", file=sys.stderr)

# if yes, split the `namespace_value` by spaces and store the resulting list in the `namespaces` dictionary with `k` as the key.

    # The _get_schema method initializes the schema to None and searches for schema reference tags in the document. 
    # If a valid schema reference is found, it sets the schema attribute to its value. It also initializes an empty 
    # namespaces dictionary and searches for the root element to extract and store namespace attributes. If valid 
    # namespaces are found, they are split by spaces and stored in the namespaces dictionary.
                        


    def _get_context_elements(
        self,
    ) -> Generator[Tag, None, None]:
        
# a method `_get_context_elements` takes `self` (the instance) as a parameter and returns a generator that yields `Tag` objects.

        resources = self.soup.find(["ix:resources", "resources"])

# search for any of the specified resources tags in the `soup` object and assigns the first occurrence to `resources`.

        if isinstance(resources, Tag):

# Check if `resources` is a Tag object.

            for s in resources.find_all(["xbrli:context", "context"]):

# if yes, iterate over all of the specified context tags within the `resources` tag.

                if isinstance(s, Tag):
                    yield s

                    # print(f"s: {s}", file=sys.stderr)

#  if  the context tag `s` is a `Tag` object, yield it.

    # The `_get_context_elements` method searches for resources tags in the document and, if found, iterates over all 
    # context tags within them. It yields each context tag as a `Tag` object, providing a generator for processing 
    # these elements.
                    


    def _get_contexts(self) -> None:

# the signature of a method named `_get_contexts` with the parameters `self` and return type `None`.

        self.contexts = {}

# set up an empty dictionary named `contexts`.

        for s in self._get_context_elements():

# loop through each context tag in the generator returned by the `_get_context_elements` method.

            if not s.get("id"):
                continue

# if the context tag does not have an `id` attribute, skip to the next iteration.

            s_id = s["id"]

# retrieve the value of the `id` attribute from the context tag and assign it to `s_id`.

            if not isinstance(s_id, str):
                continue  # pragma: no cover

# if the `id` is not a string, skip to the next iteration.

            try:

# try to execute the following block of code (which creates and stores an ixbrlContext object in the `contexts` dictionary):
                
                self.contexts[s_id] = ixbrlContext(

# create a new `ixbrlContext` object and store it in the `contexts` dictionary with `s_id` as the key.

                    _id=s_id,

# set the `_id` attribute of the `ixbrlContext` object to `s_id`.

                    entity={

# begin defining the entity dictionary.

                        "scheme": self._get_tag_attribute(s, ["xbrli:identifier", "identifier"], "scheme"),

# get the `scheme` attribute from the `identifier` tag and store it in the "scheme" key of the entity dictionary.

                        "identifier": self._get_tag_text(s, ["xbrli:identifier", "identifier"]),

# get the text content of the `identifier` tag and store it in the "identifier" key of the entity dictionary.

                    },

# end the entity dictionary definition.

                    segments=[

# begin defining the segments list.

                        {"tag": x.name, "value": x.text.strip(), **x.attrs}

# for each segment, create a dictionary with the tag name, text value, and attributes.

                        for x in self._get_tag_children(s, ["xbrli:segment", "segment"])

# iterate over each child tag of the `segment` tag within the context tag.

                    ],

# end the segments list definition.

                    instant=self._get_tag_text(s, ["xbrli:instant", "instant"]),

# get the text from the instant tag.

                    startdate=self._get_tag_text(s, ["xbrli:startDate", "startDate"]),

# get the text from the startDate tag.

                    enddate=self._get_tag_text(s, ["xbrli:endDate", "endDate"]),

# get the text from the endDate tag.

                )

                # print(f"self.contexts: {self.contexts}", file=sys.stderr)

# end the `ixbrlContext` object creation.

            except Exception as e:

# if an exception (≈ an object that represents an error) occurs during the above block of code, catch it and execute the following block.

                self.errors.append(

# add an error object to the `errors` list.

                    ixbrlError(

# create an `ixbrlError` object with the error and the context tag as attributes.

                        error=e,

# set the `error` attribute of the `ixbrlError` object to the caught exception.

                        element=s,

# set the `element` attribute of the `ixbrlError` object to the context tag (i.e. the current context element `s`).

                    )

# end the `ixbrlError` object creation.

                )

# end appending the error to the `errors` list.

                if self.raise_on_error:
                    raise

# if the `raise_on_error` flag is `True`, raise the caught exception.
                


    def _get_unit_elements(self) -> Generator[Tag, None, None]:
        resources = self.soup.find(["ix:resources", "resources"])
        if isinstance(resources, Tag):
            for s in resources.find_all(["xbrli:unit", "unit"]):
                if isinstance(s, Tag):
                    yield s

                    # print(f"s: {s}", file=sys.stderr)

# the `_get_unit_elements` method searches for resources tags in the document, and if found, iterates over all unit tags 
# within them, yielding each unit tag as a `Tag` object for further processing.
                    


    def _get_units(self) -> None:

# the signature of a method named `_get_units` that returns `None`.

# the `_get_units` method initializes the `units` dictionary and iterates over each unit tag in the generator returned
# by the `_get_unit_elements` method.

        self.units: Dict[str, Optional[str]] = {}

# set up an empty dictionary named `units` to store unit information, where the keys are strings and the values are optional strings.

        for s in self._get_unit_elements():

# loop through each element / unit tag in the generator returned by the `_get_unit_elements` method.

            s_id = s.get("id")

# get the value of the "id" attribute from the unit element / unit tag `s` and assign it to `s_id`.

            if isinstance(s_id, str):

# check if `s_id` is a string.

                self.units[s_id] = self._get_tag_text(s, ["xbrli:measure", "measure"])

                # print(f"self.units: {self.units}", file=sys.stderr)

# if `s_id` is a string, get the text content of the "measure" tag within the unit tag `s` and store it in the `units` dictionary with `s_id` as the key.
                
    # The `_get_units` method initializes an empty dictionary to store unit information. 
    # It iterates over each unit element, retrieves its "id" attribute, checks if the "id" 
    # is a string, and stores the text from the measure tag in the dictionary using the "id" as the key.
                


    def _get_tag_continuation(self, s: Union[BeautifulSoup, Tag], start_str: str = "") -> str:

# the signature of a method `_get_tag_continuation` that takes `self`, a `BeautifulSoup` or `Tag` object `s`, 
# and `start_str` (a string with a default value of an empty string) as parameters, and returns a string.

        if not isinstance(s, Tag):
            return start_str

# if `s` is not a `Tag` object, return the initial string (`start_str`) as it is.

        start_str += s.text

# add the text content of the tag `s` to the `start_str`.

        if s.attrs.get("continuedAt"):
            continuation_tag = self.soup.find(id=s.attrs.get("continuedAt"))

# If `s` has a `continuedAt` attribute, search for the tag with the id specified by the `continuedAt` attribute 
# and assign it to `continuation_tag`.

            if isinstance(continuation_tag, Tag) and continuation_tag.name == "continuation":
                return self._get_tag_continuation(continuation_tag, start_str)
            
# if `continuation_tag` is a `Tag` object and its name is "continuation", call `_get_tag_continuation` again with
# `continuation_tag` and the updated `start_str`, and return the result.

        return start_str
    
# return the final `start_str` after all continuations have been processed.

    # The `_get_tag_continuation` method retrieves the complete text from a tag and its continuation tags. It appends the text 
    # of the given tag to `start_str`, checks for a `continuedAt` attribute, and if found, recursively processes the continuation tag. 
    # The method returns the concatenated text string once all continuations are processed.



    def _get_nonnumeric(self) -> None:

# the signature of a method named `_get_nonnumeric` that returns `None`.

        self.nonnumeric = []

# set up an empty list named `nonnumeric` to store non-numeric elements.

        for s in self.soup.find_all({"nonNumeric"}):

# loop through each non-numeric element in the document (the soup object).

            try:

# try to execute the following block of code (which handles potential errors / catches exceptions):

                context = self.contexts.get(s["contextRef"], s["contextRef"])

# retrieve the context reference from the `contexts` dictionary using the `contextRef` attribute of the non-numeric element `s` and assign it to `context`.

                format_ = s.get("format")

# get the value of the `format` attribute from the non-numeric element `s`.

                if not isinstance(format_, str):
                    format_ = None

# if the `format_` is not a string, set it to `None`.

                exclusion = s.find("exclude")

# search for an `exclude` tag within the element `s` and assign it to `exclusion`.

                if exclusion is not None:
                    exclusion.extract()

# if `exclusion` is not `None` (i.e., the exclude tag is found), remove the `exclusion` tag from the element `s`.

                text = s.text

# get the text content of the element `s` and assign it to the variable `text`.

                if s.attrs.get("continuedAt"):
                    text = self._get_tag_continuation(s)

# if the element `s` has a `continuedAt` attribute, call the `_get_tag_continuation` method with `s` to get the complete text content, including any continuations.

                self.nonnumeric.append(
                    ixbrlNonNumeric(
                        context=context, 
                        # set the `context` attribute of the `ixbrlNonNumeric` object to `context`.
                        name=s["name"] if isinstance(s["name"], str) else "",
                        # set the `name` attribute of the `ixbrlNonNumeric` object to the value of 
                        # the `name` attribute of the element `s` if it is a string; otherwise, set it to an empty string.
                        format_=format_,
                        # set the `format_` attribute of the `ixbrlNonNumeric` object to `format_`.
                        value=text.strip().replace("\n", "") if isinstance(text, str) else "",
                        # set the `value` attribute the `ixbrlNonNumeric` object to the stripped and newline-free text, or 
                        # an empty string if `text` is not a string.
                        soup_tag=s,
                        # set the `soup_tag` attribute of the `ixbrlNonNumeric` object to the element `s`.
                    )
                    # create a new `ixbrlNonNumeric` object.
                )
                # append the `ixbrlNonNumeric` object to the `nonnumeric` list.

            except Exception as e:

# if an exception occurs during the above block of code, catch it and execute the following block.

                self.errors.append(
                    ixbrlError(
                        error=e,
                        element=s,
                    )
                )

# create a new error object `ixbrlError` (with an `error` attribute set to the exception `e` and an `element` attribute set to the element `s`)
# and add it to the `errors` list.
                
                if self.raise_on_error:
                    raise

# if the `raise_on_error` flag is `True`, raise the caught exception.
                
    # The `_get_nonnumeric` method initializes an empty list for non-numeric elements and iterates over all `nonNumeric` tags in the document. 
    # For each tag, it retrieves the context reference and format, removes any `exclude` tags, and handles text continuations. It creates and 
    # appends an `ixbrlNonNumeric` object to the list, logging and optionally raising errors encountered during processing.
                


    def _get_numeric(self) -> None:

# the signature of a method named `_get_numeric` that returns `None`.

        self.numeric = []

# set up an empty list named `numeric` to store numeric elements.

        for s in self.soup.find_all({"nonFraction"}):

# loop through each element found with the tag `nonFraction` in the document (the soup object).

            try:

# try to execute the following block of code (which catches and handles exceptions -- i.e. errors):

                self.numeric.append(
                    # append a new `ixbrlNumeric` object to the `numeric` list.
                    ixbrlNumeric(
                        # create a new `ixbrlNumeric` object with the following attributes:
                        text=s.text,
                        # the `text` attribute is set to the text content of the element `s`.
                        context=self.contexts.get(s["contextRef"], s["contextRef"]),
                        # the `context` attribute is set to the context reference, which is retrieved from the `contexts` dictionary 
                        # using the `contextRef` attribute of the element `s`.
                        unit=self.units.get(s["unitRef"], s["unitRef"]),
                        # the `unit` attribute is set to the unit reference, which is retrieved from the `units` dictionary using the
                        # `unitRef` attribute of the element `s`.
                        soup_tag=s,
                        # the `soup_tag` attribute is set to the element `s`.
                        **s.attrs,
                        # unpack and include all other attributes of the element `s`.
                    )
                )
            except Exception as e:
                # if an exception (error) occurs during the above block of code, catch it and execute the following block to handle it.
                self.errors.append(
                    ixbrlError(
                        error=e,
                        element=s,
                    )
                )
                # create a new error object `ixbrlError` with the caught exception `e` and the element `s`, and add it to the `errors` list.
                if self.raise_on_error:
                    raise
                # if the `raise_on_error` flag is `True`, raise the caught exception.

    # The `_get_numeric` method initializes an empty list for numeric elements and iterates over all `nonFraction` tags in the document. 
    # For each tag, it creates an `ixbrlNumeric` object with the text, context reference, unit reference, and other attributes, and appends 
    # it to the list. It handles and logs any exceptions that occur, raising them if the `raise_on_error` flag is set.















class XBRLParser(IXBRLParser):
    root_element = "xbrl"

    def _get_context_elements(self) -> Generator[Tag, None, None]:
        # the signature of a method named `_get_context_elements` that returns a generator yielding `Tag` objects.
        for s in self.soup.find_all(["xbrli:context", "context"]):
            # iterate over all elements found with the tags `xbrli:context` or `context` in the document / soup object.
            if isinstance(s, Tag):
                yield s
                # if the element `s` is an instance of the `Tag` class, yield the `Tag` object `s`.

    def _get_unit_elements(self) -> Generator[Tag, None, None]:
        for s in self.soup.find_all(["xbrli:unit", "unit"]):
            if isinstance(s, Tag):
                yield s

    def _get_elements(self) -> Generator[Tag, None, None]:
        resource = self.soup.find(self.root_element)
        if isinstance(resource, Tag):
            for s in resource.find_all(True):
                if isinstance(s, Tag):
                    yield s

    # The `_get_elements` method method returns a generator that produces `Tag` objects representing elements within the root element.
    # It works by finding the root element in the `soup` object and if it is an instance of the `Tag` class, iterating over all tag elements within it. 
    # It yields each tag element found that is an instance of the `Tag` class. 
                    

    def _get_numeric(self) -> None:
        self.numeric = []
        for s in self._get_elements():
            if not s.get("contextRef") or not s.get("unitRef"):
                continue
            context_ref = s["contextRef"]
            unit_ref = s["unitRef"]
            if not isinstance(context_ref, str) or not isinstance(unit_ref, str):
                continue  # pragma: no cover
            try:
                self.numeric.append(
                    ixbrlNumeric(
                        name=s.name,
                        text=s.text,
                        context=self.contexts.get(context_ref, context_ref),
                        unit=self.units.get(unit_ref, unit_ref),
                        soup_tag=s,
                        **s.attrs,
                    )
                )
            except Exception as e:
                self.errors.append(
                    {
                        "error": e,
                        "element": s,
                    }
                )
                if self.raise_on_error:
                    raise

    def _get_nonnumeric(self) -> None:
        self.nonnumeric = []
        for s in self._get_elements():
            try:
                if not s.get("contextRef") or s.get("unitRef"):
                    continue
                context_ref = s["contextRef"]
                if not isinstance(context_ref, str):
                    continue  # pragma: no cover
                context = self.contexts.get(context_ref, context_ref)
                format_ = s.get("format")
                if not isinstance(format_, str):
                    format_ = None

                text = s.text

                self.nonnumeric.append(
                    ixbrlNonNumeric(
                        context=context,
                        name=s.name if isinstance(s.name, str) else "",
                        format_=format_,
                        value=text.strip().replace("\n", "") if isinstance(text, str) else "",
                        soup_tag=s,
                    )
                )
            except Exception as e:
                self.errors.append(
                    ixbrlError(
                        error=e,
                        element=s,
                    )
                )
                if self.raise_on_error:
                    raise


class IXBRL:
    """
    Parse an iXBRL file.
    """

    def __init__(self, f: IO, raise_on_error: bool = True) -> None:  # noqa: FBT001, FBT002
        """Constructor for the IXBRL class.

        Parameters:
            f:  File-like object to parse.
            raise_on_error:  Whether to raise an exception on error
        """
        self.soup = BeautifulSoup(f.read(), "xml", multi_valued_attributes=None)
        self.raise_on_error = raise_on_error
        self._get_parser()
        self.parser._get_schema()
        self.parser._get_contexts()
        self.parser._get_units()
        self.parser._get_nonnumeric()
        self.parser._get_numeric()

    @classmethod
    def open(cls, filename: Union[str, Path], raise_on_error: bool = True):  # noqa: FBT001, FBT002
        """Open an iXBRL file.

        Parameters:
            filename:  Path to file to parse.
            raise_on_error:  Whether to raise an exception on error
        """
        with open(filename, "rb") as a:
            return cls(a, raise_on_error=raise_on_error)

    def _get_parser(self) -> None:
        if self.soup.find("html"):
            self.filetype = FILETYPE_IXBRL
            parser = IXBRLParser
        elif self.soup.find("xbrl"):
            self.filetype = FILETYPE_XBRL
            parser = XBRLParser
        else:
            msg = "Filetype not recognised"
            raise IXBRLParseError(msg)
        self.parser: BaseParser = parser(self.soup, raise_on_error=self.raise_on_error)

    def __getattr__(self, name: str):
        return getattr(self.parser, name)

    def to_json(self) -> Dict:
        """Return a JSON representation of the iXBRL file.

        Returns:
            A dictionary containing the following keys:

                - schema:  The schema used in the iXBRL file.
                - namespaces:  The namespaces used in the iXBRL file.
                - contexts:  The contexts used in the iXBRL file.
                - units:  The units used in the iXBRL file.
                - nonnumeric:  The non-numeric elements in the iXBRL file.
                - numeric:  The numeric elements in the iXBRL file.
                - errors:  The number of errors encountered when parsing the iXBRL file.
        """
        return {
            "schema": self.schema,
            "namespaces": self.namespaces,
            "contexts": {c: ct.to_json() for c, ct in self.contexts.items()},
            "units": self.units,
            "nonnumeric": [a.to_json() for a in self.nonnumeric],
            "numeric": [a.to_json() for a in self.numeric],
            "errors": len(self.errors),
        }

    def to_table(self, fields: str = "numeric") -> List[Dict]:
        """Return a list of dictionaries representing the iXBRL file.

        This is suitable for passing to pandas.DataFrame.from_records().

        Parameters:
            fields:  Which fields to include in the output.  Can be "numeric", "nonnumeric" or "all".

        Returns:
            A list of dictionaries representing the iXBRL file.

        The fields included are:

        - schema (str)
        - name (str) -- the name of the element
        - value -- the value of the element. Can be number, str, None, or boolean
        - unit (str) -- the unit of the element if present
        - instant (date) -- the instant date of the element context if present
        - startdate (date) -- the start date of the element context if present
        - enddate (date) -- the end date of the element context if present
        - segment:N (str) -- the Nth segment of the element context if present (can be repeated)

        Examples:
            >>> import pandas as pd
            >>> i = IXBRL.open("tests/fixtures/ixbrl/uk-gaap/2009-12-31/Company-Accounts-Data.xml")
            >>> df = pd.DataFrame.from_records(i.to_table(fields="numeric"))
            >>> df.head()
        """

        # print(f"fields: {fields}", file=sys.stderr)

        if fields == "nonnumeric":
            values = self.nonnumeric
        elif fields == "numeric":
            values = self.numeric
        else:
            values = self.nonnumeric + self.numeric

        # print(f"values: {values}", file=sys.stderr)

        ret = []
        for v in values:
            # print "v: {v}" to stderr
            # print(f"v: {v}", file=sys.stderr)
            
            if isinstance(v.context, ixbrlContext) and v.context.segments:
                segments = {
                    f"segment:{i}": "{} {} {}".format(s.get("tag", ""), s.get("dimension"), s.get("value")).strip()
                    for i, s in enumerate(v.context.segments)
                }

                # print(f"segments: {segments}", file=sys.stderr)

# Check if the `context` of `v` is an instance of the `ixbrlContext` class 
# and if the `context` contains any segments.
                
# If both conditions are met, create a new dictionary called `segments`.
                
# For each segment in the context, add an entry to the `segments` dictionary. 
# The key for each entry will be a string that says "segment" followed by the 
# segment's index number (starting from 0). The value for each entry will be 
# a string that combines the `tag`, `dimension`, and `value` of the segment, with 
# any extra spaces at the beginning or end removed.
                
# This process will be repeated for each segment in the context, building the 
# `segments` dictionary with all relevant information.

            else:
                segments = {"segment:0": ""}

# If the `context` of `v` is not an instance of `ixbrlContext` or if there are no  
# segments in the `context`, create a dictionary called `segments` with one entry  
# where the key is `"segment:0"` and the value is an empty string.

            ret.append(
                {
# Add a new dictionary to the `ret` list with the following key-value pairs:
                    "schema": " ".join(self.namespaces.get(f"xmlns:{v.schema}", [v.schema])),
# the key "schema" with the value being the schema of the iXBRL file, joined:
# the schema is obtained from the `namespaces` dictionary using the schema of the
# current value `v` as the key; the value is a list of namespaces, which is joined
# into a single string using a space as the separator.

# OR:

# Create a key named "schema" and set its value to the namespace associated with 
# v.schema. If the namespace is not found, use v.schema as the value. The namespace 
# is joined into a single string if it's a list.
                    "name": v.name,
# the key "name" with the value being the name of the element v
                    "value": v.value,
# the key "value" with the value being the value of the element v
                    "unit": v.unit if hasattr(v, "unit") else None,
# the key "unit" with the value being the unit of the element v if it exists;
                    "instant": str(v.context.instant)
                    if isinstance(v.context, ixbrlContext) and v.context.instant
                    else None,
# the key "instant" with the value being the string representation of 
# `v.context.instant` (i.e., the instant date of the element context) 
# if `v.context` is an instance of `ixbrlContext` and `v.context.instant` is present; 
# otherwise, it is set to None
                    "startdate": str(v.context.startdate)
                    if isinstance(v.context, ixbrlContext) and v.context.startdate
                    else None,
# the key "startdate" with the value being the string representation of
# `v.context.startdate` (i.e., the start date of the element context) if
# `v.context` is an instance of `ixbrlContext` and `v.context.startdate` is present;
# otherwise, it is set to None
                    "enddate": str(v.context.enddate)
                    if isinstance(v.context, ixbrlContext) and v.context.enddate
                    else None,
# the key "enddate" with the value being the string representation of
# `v.context.enddate` (i.e., the end date of the element context) if
# `v.context` is an instance of `ixbrlContext` and `v.context.enddate` is present;
# otherwise, it is set to None
                    **segments,
# add all key-value pairs from the `segments` dictionary to the new dictionary
                }
            )
        return ret
# Return the list of dictionaries created for each value in the `values` list
    
