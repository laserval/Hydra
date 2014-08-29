package com.findwise.hydra.stage;

import com.findwise.hydra.DocumentFile;
import com.findwise.hydra.local.Local;
import com.findwise.hydra.local.LocalDocument;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Stage
public class AttachmentReaderStage extends AbstractProcessStage {

	@Override
	public void process(LocalDocument document) throws ProcessException {
		List<String> attachmentContents = new ArrayList<String>();
		try {
			for (DocumentFile<Local> file : getRemotePipeline().getFiles(document.getID())) {
				String contents = null;
				try {
					contents = IOUtils.toString(file.getStream(), file.getEncoding());
				} catch (IOException e) {
					e.printStackTrace();
				}
				attachmentContents.add(contents);
			}
		} catch (IOException e) {
			throw new ProcessException("Failed to fetch attachments", e);
		}
		document.putContentField("attachmentContents", attachmentContents);
	}
}
