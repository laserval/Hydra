package com.findwise.hydra.stage;

import com.findwise.hydra.DocumentFile;
import com.findwise.hydra.local.Local;
import com.findwise.hydra.local.LocalDocument;
import org.apache.commons.io.IOUtils;

import java.util.ArrayList;
import java.util.List;

@Stage
public class AttachmentReaderStage extends AbstractProcessStage {

	@Override
	public void process(LocalDocument document) throws Exception {
		List<String> attachmentContents = new ArrayList<String>();
		for (DocumentFile<Local> file : document.getFiles()) {
			String contents = IOUtils.toString(file.getStream(), file.getEncoding());
			attachmentContents.add(contents);
		}
		document.putContentField("attachmentContents", attachmentContents);
	}
}
