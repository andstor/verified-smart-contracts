import pathlib
import sys
parser_dir = pathlib.Path(__file__).parent.parent / 'solidity-universal-parser' / 'parser'
sys.path.append(str(parser_dir))
from python3.SolidityVisitor import SolidityVisitor
from python3.SolidityParser import SolidityParser
from python3.SolidityLexer import SolidityLexer


class CommentVisitor(SolidityVisitor):

    COMMENT_TYPES = {
        SolidityLexer.LINE_COMMENT: "LineComment",
        SolidityLexer.COMMENT: "Comment",
        SolidityLexer.NatSpecSingleLine: "NatSpecSingleLine",
        SolidityLexer.NatSpecMultiLine: "NatSpecMultiLine"
    }

    def visitContractDefinition(self, ctx: SolidityParser.ContractDefinitionContext):
        token = ctx.parser.getTokenStream().get(ctx.start.tokenIndex-1)
        contract_data = {}
        contract_data["class_name"] = ctx.identifier().getText()
        contract_data["class_code"] = self.extract_original_text(ctx)
        if (token.channel == 1):
            comment = self.extractComment(ctx)
            contract_data["class_documentation"] = comment["text"]
            contract_data["class_documentation_type"] = comment["type"]

        children = self.visitChildren(ctx)
        for child in children:
            child["class_name"] = contract_data["class_name"]
            child["class_code"] =  contract_data["class_code"]
            child["class_documentation"] = contract_data.get("class_documentation", "")
            child["class_documentation_type"] = contract_data.get("class_documentation_type", "")
        return children

    def visitFunctionDefinition(self, ctx: SolidityParser.FunctionDefinitionContext):
        token = ctx.parser.getTokenStream().get(ctx.start.tokenIndex-1)
        func_data = {}
        func_data["func_name"] = ctx.identifier().getText() if ctx.identifier() else ""
        func_data["func_code"] = self.extract_original_text(ctx)
        if (token.channel == 1):
            comment = self.extractComment(ctx)
            func_data["func_documentation"] = comment["text"]
            func_data["func_documentation_type"] = comment["type"]
        else:
            func_data["func_documentation"] = ""
            func_data["func_documentation_type"] = ""
        
        return self.aggregateResult([func_data], self.visitChildren(ctx))

    def extractComment(self, ctx):
        stream = ctx.parser.getTokenStream()
        line_comment_types = [
            SolidityLexer.NatSpecSingleLine, SolidityLexer.LINE_COMMENT]
        multiline_comment_types = [
            SolidityLexer.COMMENT, SolidityLexer.NatSpecMultiLine]

        comment = {}
        prev_type = None

        i = 1
        while stream.get(ctx.start.tokenIndex-i).channel == 1:
            token = stream.get(ctx.start.tokenIndex-i)
            token_type_str = CommentVisitor.COMMENT_TYPES[token.type]

            if prev_type is not None:
                if token.type != prev_type or token.type not in line_comment_types:
                    break

            if token.type in line_comment_types:
                if token.type == prev_type:
                    comment["text"] = token.text + '\n' + comment["text"]
                else:
                    comment = {"text": token.text, "type": token_type_str}

            elif (token.type in multiline_comment_types):
                text = self.normalize_multiline(token.text)
                comment = {"text": text, "type": token_type_str}

            prev_type = token.type
            i += 1

        return comment

    def normalize_multiline(self, text):
        token_list = text.split('\n')
        token_list = [i.strip(' ') for i in token_list]
        normalized_token = '\n '.join(token_list)
        return normalized_token

    def extract_original_text(self, ctx):
        token_source = ctx.start.getTokenSource()
        input_stream = token_source.inputStream
        start, stop = ctx.start.start, ctx.stop.stop
        return input_stream.getText(start, stop)

    def defaultResult(self):
        return []

    def aggregateResult(self, aggregate, nextResult):
        aggregate.extend(nextResult)
        return aggregate

    def shouldVisitNextChild(self, node, currentResult):
        return True

    def visitChildren(self, node):
        result = self.defaultResult()
        n = node.getChildCount()
        for i in range(n):
            if not self.shouldVisitNextChild(node, result):
                return result

            c = node.getChild(i)
            childResult = c.accept(self)
            result = self.aggregateResult(result, childResult)

        return result
