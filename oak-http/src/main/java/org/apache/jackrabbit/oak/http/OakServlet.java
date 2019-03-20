/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.http;

import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.STRING;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map.Entry;

import javax.jcr.Credentials;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;
import javax.jcr.nodetype.PropertyDefinition;
import javax.security.auth.login.LoginException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.fileupload.util.Streams;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.UUIDUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.spi.nodetype.EffectiveNodeType;
import org.apache.jackrabbit.util.Base64;
import org.apache.tika.Tika;
import org.apache.tika.mime.MediaType;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;

public class OakServlet extends HttpServlet {

    private static final MediaType JSON =
            MediaType.parse("application/json");

    private static final MediaType SMILE =
            MediaType.parse("application/x-jackson-smile");
    
    private static final Tika TIKA = new Tika();

    private static final Representation[] REPRESENTATIONS = {
        new HtmlRepresentation(),
        new TextRepresentation(),
        new JsonRepresentation(JSON, new JsonFactory()),
        new JsonRepresentation(SMILE, new SmileFactory()),
        new PostRepresentation() };

    private final ContentRepository repository;

    public OakServlet(ContentRepository repository) {
        this.repository = repository;
    }

    @Override
    protected void service(
            HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        try {
            Credentials credentials = null;

            String authorization = request.getHeader("Authorization");
            if (authorization != null && authorization.startsWith("Basic ")) {
                String[] basic =
                        Base64.decode(authorization.substring("Basic ".length())).split(":");
                credentials = new SimpleCredentials(basic[0], basic[1].toCharArray());
            } else {
                throw new LoginException();
            }

            ContentSession session = repository.login(credentials, null);
            try {
                Root root = session.getLatestRoot();
                request.setAttribute("root", root);

                // Find the longest part of the given path that matches
                // an existing node. The tail part might be used when
                // creating new nodes or when exposing virtual resources.
                // Note that we need to traverse the path in reverse
                // direction as some parent nodes may be read-protected.
                String head = request.getPathInfo();
                String tail = "";
                Tree tree = root.getTree(head);
                while (!tree.exists()) {
                    if (tree.isRoot()) {
                        response.sendError(HttpServletResponse.SC_NOT_FOUND);
                        return;
                    }
                    tail = "/" + tree.getName() + tail;
                    tree = tree.getParent();
                }
                request.setAttribute("tree", tree);
                request.setAttribute("path", tail);

                super.service(request, response);
            } finally {
                session.close();
            }
        } catch (NoSuchWorkspaceException e) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
        } catch (LoginException e) {
            response.setHeader("WWW-Authenticate", "Basic realm=\"Oak\"");
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
        }
    }

    @Override
    protected void doGet(
            HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        AcceptHeader accept = new AcceptHeader(request.getHeader("Accept"));
        Representation representation = accept.resolve(REPRESENTATIONS);

        String path = (String) request.getAttribute("path");
        if (path.isEmpty()) {
            Tree tree = (Tree) request.getAttribute("tree");
            representation.render(tree, response);
        } else {
            Tree tree = (Tree) request.getAttribute("tree");
            String name = path;
            if(name.startsWith("/")) {
                name = name.substring(1);
            }
            
            if(tree.hasProperty(name)) {
                
                PropertyState property = tree.getProperty(name);
                if(BINARY.equals(property.getType()) && tree.hasProperty("jcr:mimeType")) {
                    // Binary property with a known mime type is requested.
                    // Just write it back to response
                    String mime = tree.getProperty("jcr:mimeType").getValue(STRING);
                    response.setContentType(mime);
                    InputStream in = null;
                    try {
                        org.apache.jackrabbit.oak.api.Blob value = property.getValue(BINARY);
                        String ref = value.getReference();
                        System.out.println("Blob reference = " + ref);
                        in = value.getNewStream();
                        IOUtils.copy(in, response.getOutputStream());
                    } finally {
                        IOUtils.closeQuietly(in);
                    }
                  
                } else { 
                    representation.render(property, response);
                }
                
            } else {
                // There was an extra path component that didn't match
                // any existing nodes or properties, so we just send a 404 response.
                response.sendError(HttpServletResponse.SC_NOT_FOUND);
            }
        }
    }

    @Override
    protected void doPost(
            HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        try {
            Root root = (Root) request.getAttribute("root");
            Tree tree = (Tree) request.getAttribute("tree");
            String path = (String) request.getAttribute("path");
            boolean postProcessed = false;

            for (String name : PathUtils.elements(path)) {
                tree = tree.addChild(name);
            }
            
            if(ServletFileUpload.isMultipartContent(request)) {
                doUpload(request, response, tree);
                postProcessed = true;
            } else {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = mapper.readTree(request.getInputStream());
                if (node != null && node.isObject()) {
                    post(node, tree, root);
                    postProcessed = true;
                } 
            }
            
            if(postProcessed) {
                root.commit();
                request.setAttribute("path", "");
                request.setAttribute("tree", tree);
                doGet(request, response);
            } else {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST);
            }
        } catch (CommitFailedException e) {
            throw new ServletException(e);
        } catch (RepositoryException e) {
            throw new ServletException(e);
        }
    }

    private void post(JsonNode node, Tree tree, Root root) throws IllegalArgumentException, RepositoryException {
        Iterator<Entry<String, JsonNode>> iterator = node.fields();
        while (iterator.hasNext()) {
            Entry<String, JsonNode> entry = iterator.next();
            String name = entry.getKey();
            JsonNode value = entry.getValue();
            if (value.isObject()) {
                if (tree.hasProperty(name)) {
                    tree.removeProperty(name);
                }
                Tree child = tree.getChild(name);
                if (!child.exists()) {
                    child = tree.addChild(name);
                }
                post(value, child, root);
            } else {
                Tree child = tree.getChild(name);
                if (child.exists()) {
                    child.remove();
                }
                if (value.isNull()) {
                    tree.removeProperty(name);
                } else if (value.isBoolean()) {
                    tree.setProperty(name, value.asBoolean());
                } else if (value.isInt()) {
                    tree.setProperty(name, value.asInt());
                } else if (value.isLong()) {
                    tree.setProperty(name, value.asLong());
                } else if (value.isDouble()) {
                    tree.setProperty(name, value.asDouble());
                } else if (value.isBigDecimal()) {
                    tree.setProperty(name, value.decimalValue());
                } else {
                    tree.setProperty(name, value.asText(), getValidPropertyType(root, tree, name));
                }
            }
        }
    }
    
    private void doUpload(HttpServletRequest request, HttpServletResponse response, Tree tree)
            throws ServletException, IOException, IllegalArgumentException, RepositoryException {
        ServletFileUpload upload = new ServletFileUpload();
        Root root = (Root) request.getAttribute("root");
        
        try {
            FileItemIterator iter = upload.getItemIterator(request);
            Tree parent = tree;
            
            PostRequestFieldsMapper fieldsMapper = new PostRequestFieldsMapper();
            while(iter.hasNext()) {
                final FileItemStream item = iter.next();
                String name = item.getFieldName();
                String itemName = item.getName();
                InputStream stream = item.openStream();
                
                if(item.isFormField()) {
                    String value = Streams.asString(stream);
                    fieldsMapper.writeField(name, value);
                } else {
                    Tree child = parent.addChild(itemName);
                    child.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_FILE, Type.NAME);
                    Tree content = child.addChild(JcrConstants.JCR_CONTENT);
                    content.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_RESOURCE, Type.NAME);
                    content.setProperty(JcrConstants.JCR_DATA, root.createBlob(stream), Type.BINARY);
                    content.setProperty(JcrConstants.JCR_UUID, UUIDUtils.generateUUID(content.getPath()));
                   
                    String detectedMimeType = TIKA.detect(itemName);
                    if(detectedMimeType != null && detectedMimeType.length() > 0) {
                        content.setProperty(JcrConstants.JCR_MIMETYPE, detectedMimeType, Type.STRING);
                    }
                }
            }
            
            JsonNode node = fieldsMapper.toJsonNode(); 
            if (node != null && node.isObject()) {
                post(node, tree, root);
            }
            
        } catch (FileUploadException e) {
            throw new ServletException(e);
        } 
    }
    
    private Type getValidPropertyType(Root root, Tree tree, String name) throws RepositoryException {
        ReadOnlyNodeTypeManager ntm = ReadOnlyNodeTypeManager.getInstance(root, NamePathMapper.DEFAULT);
        
        EffectiveNodeType ent = null;
        if(tree.hasProperty(JcrConstants.JCR_PRIMARYTYPE)) {
            ent = ntm.getEffectiveNodeType(tree);
        } else {
            // Node does not have a primary type. Must be new node. Use parent node.
            ent = ntm.getEffectiveNodeType(tree.getParent());
        }
        
        Iterator<PropertyDefinition> it = ent.getPropertyDefinitions().iterator();
        while(it.hasNext()) {
            PropertyDefinition pd = it.next();
            if(pd.getName().equals(name)) {
                Type<?> t = Type.fromTag(pd.getRequiredType(), false);
                return t;
            }
        }
        
        return Type.STRING;
    }

    @Override
    protected void doDelete(
            HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        try {
            Root root = (Root) request.getAttribute("root");
            Tree tree = (Tree) request.getAttribute("tree");
            if (!tree.isRoot()) {
                Tree parent = tree.getParent();
                Tree child = parent.getChild(tree.getName());
                if (child.exists()) {
                    child.remove();
                }
                root.commit();
                response.sendError(HttpServletResponse.SC_OK);
            } else {
                // Can't remove the root node
                response.sendError(HttpServletResponse.SC_FORBIDDEN);
            }
        } catch (CommitFailedException e) {
            throw new ServletException(e);
        }
    }

}
