import React, { useState } from "react";
import Box from "@mui/material/Box";
import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import IconButton from "@mui/material/IconButton";
import CloseIcon from "@mui/icons-material/Close";
import { Stack } from "@mui/material";

function SentimentAnalysisPopup({ document, onClose, increaseZIndex, zIndex }) {
  // Simple error handling popup
  return (
    <Paper
      elevation={3}
      sx={{
        position: "fixed",
        right: 20 + 150 * (document?.index || 0),
        bottom: 0,
        minWidth: "50%",
        width: "fit-content",
        zIndex: zIndex,
        maxHeight: "calc(100% - 40px)",
        display: "flex",
        flexDirection: "column",
        gap: 2,
        borderRadius: 2,
      }}
      className="sentimentAnalysisPopup"
      onClick={increaseZIndex}
    >
      <Stack spacing={3} sx={{ width: "-webkit-fill-available" }}>
        {/* Header */}
        <Box display="flex" justifyContent="space-between" alignItems="center" className="sentimentAnalysisHeader">
          <Typography variant="h6" className="sentimentAnalysisDocumentTitle">
            {document?.id || 'Unknown Document'}
          </Typography>
          <IconButton size="small" onClick={() => onClose(document?.id)} className="sentimentAnalysisCloseIcon">
            <CloseIcon />
          </IconButton>
        </Box>

        <Stack spacing={3} sx={{ overflow: "auto", height: "85vh", padding: 3 }}>
          <Box className="clusterContainer" sx={{ padding: 3, textAlign: 'center' }}>
            <Typography variant="h6" sx={{ color: '#666666' }}>
              Analysis data is not available for this document
            </Typography>
            <Typography variant="body2" sx={{ color: '#666666', mt: 2 }}>
              The document was processed successfully, but analysis data could not be generated.
              This might happen if the document has no comments or if there was an issue during the analysis phase.
            </Typography>
          </Box>
        </Stack>
      </Stack>
    </Paper>
  );
}

export default SentimentAnalysisPopup;